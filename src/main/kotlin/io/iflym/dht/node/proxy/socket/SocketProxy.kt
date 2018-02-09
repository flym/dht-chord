package io.iflym.dht.node.proxy.socket

import com.google.common.collect.Maps
import com.iflym.core.util.ExceptionUtils
import com.iflym.core.util.StringUtils
import io.iflym.dht.data.PreAndSucccessorsAndEntries
import io.iflym.dht.data.PreAndSuccessors
import io.iflym.dht.exception.CommunicationException
import io.iflym.dht.model.Entry
import io.iflym.dht.model.Id
import io.iflym.dht.model.Key
import io.iflym.dht.model.Url
import io.iflym.dht.net.Endpoint
import io.iflym.dht.node.RingNode
import io.iflym.dht.node.proxy.AbstractProxyNode
import io.iflym.dht.node.proxy.socket.data.*
import io.iflym.dht.node.proxy.socket.data.MethodValue.*
import io.iflym.dht.node.proxy.socket.protocal.JsonDecoder
import io.iflym.dht.node.proxy.socket.protocal.JsonEncoder
import io.iflym.dht.service.ServiceStatus
import io.iflym.dht.util.ThreadFactoryUtils
import io.iflym.dht.util.loggerFor
import io.iflym.dht.util.withLock
import io.netty.bootstrap.Bootstrap
import io.netty.channel.*
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioSocketChannel
import java.net.InetSocketAddress
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantLock
import java.util.function.Consumer

/**
 * 实现基于socket访问协议的代理处理
 *
 * @author flym
 */
class SocketProxy private constructor(
        /**
         * 本地链接地址,即从哪儿连过去.可以是一个本地存储节点的地址,也可以是一个描述客户端地址的信息.
         * 即此数据在一定程度上并不承担业务含义.仅在表示节点间请求时才有业务意义(用于描述lookup请求)
         */
        private var urlOfLocalNode: Url,
        url: Url,
        nodeID: Id
) : AbstractProxyNode(nodeID, url) {

    //---------------------------- 通信相关 start ------------------------------//

    /** 网络通信锁 */
    private val netLock = ReentrantLock()
    private val netLockCondition = netLock.newCondition()

    /**
     * 描述当前发送的是第X次请求,即请求数计数.同时在进行请求创建时使用此作为惟一标识一部分
     *
     * @see [createRequestIdentifier]
     */
    private var requestCounter: Long = -1

    /** 描述响应的数据信息, 用于映射不同的请求和响应  */
    @Transient
    private val responses = Maps.newHashMap<String, Response<*>>()

    /** 映射正在等待响应的线程信息  */
    @Transient
    private val waitingThreads = Maps.newHashMap<String, WaitingThread>()

    //---------------------------- 通信相关 start ------------------------------//

    //---------------------------- 连接通道相关 start ------------------------------//

    /** 当前通道锁 */
    private val avaibleLock = ReentrantLock()
    private val avaibleCondition = avaibleLock.newCondition()

    /** 用于第一次连接时使用(初始连接只连一次)  */
    private val firstConnect = AtomicBoolean(true)

    /** 当前是否临时可用(用于处理临时掉线之间的间隔期)  */
    @Volatile
    private var isCurrentAvaible: Boolean = false

    /** 描述此远端服务已经挂掉,不可再重连  */
    @Volatile
    override var isServerDown: Boolean = false

    /** 相应的客户端通道  */
    private var channel: Channel? = null

    //---------------------------- 连接通道相关 start ------------------------------//

    /** 当前客户端  */
    private var bootstrap: Bootstrap? = null

    override val isCurrentAlive: Boolean
        get() {
            tryMakeSocketAvailable()

            return isCurrentAvaible
        }

    private fun lockChannelAndAwait(waitTime: Long) {
        withLock(avaibleLock) { avaibleCondition.await(waitTime, TimeUnit.MILLISECONDS) }
    }

    private fun lockChannelAndSignal() {
        withLock(avaibleLock) { avaibleCondition.signalAll() }
    }

    private fun selfOrProxy(nodeInfo: RemoteNodeInfo): RingNode {
        //本身就是当前节点,则返回自己(不再作代理)
        val removeNodeUrl = nodeInfo.nodeUrl
        return if (removeNodeUrl == this.urlOfLocalNode) {
            Endpoint.getEndpoint(this.urlOfLocalNode).node
        } else {
            //使用相应的代理节点表示
            createKnown(this.urlOfLocalNode, removeNodeUrl, nodeInfo.nodeId)
        }
    }

    /** 初始必要的参数  */
    override fun init() {
        bootstrap = Bootstrap().group(NioEventLoopGroup()).channel(NioSocketChannel::class.java)
                .remoteAddress(InetSocketAddress(nodeUrl.host, nodeUrl.port))
                .handler(object : ChannelInitializer<SocketChannel>() {
                    @Throws(Exception::class)
                    override fun initChannel(ch: SocketChannel) {
                        val pipeline = ch.pipeline()

                        pipeline.addLast(SocketWatchdog(this@SocketProxy))
                        pipeline.addLast(JsonDecoder())
                        pipeline.addLast(object : SimpleChannelInboundHandler<Response<*>>() {
                            @Throws(Exception::class)
                            override fun channelRead0(ctx: ChannelHandlerContext, msg: Response<*>) {
                                responseReceived(msg)
                            }
                        })
                        pipeline.addLast(JsonEncoder())
                    }
                })
    }

    /**
     * 往目标地址发送相应的请求信息
     *
     *
     * 实现层面,当前实现为同步以保证不会出现其它线程干扰问题
     *
     * @param request 请求发送信息
     */
    @Synchronized
    @Throws(CommunicationException::class)
    private fun send(request: Request) {
        try {
            log.debug("Sending request {}", request.replyWith)
            channel!!.writeAndFlush(request)
        } catch (e: Exception) {
            throw CommunicationException("发送请求数据出错.目标地址: " + this.nodeUrl + ",错误信息:" + e.message, e)
        }
    }

    /**
     * 为每一个请求信息生成一个惟一的请求标识符
     *
     *
     * 此方法是同步的,以保证计数器工作正常
     *
     * @param methodValue 用于描述请求的方法的方法定义符
     */
    @Synchronized
    private fun createRequestIdentifier(methodValue: MethodValue): String {
        //格式 当前时间-计数器-方法
        val uid = StringBuilder()
        uid.append(System.currentTimeMillis())
        uid.append("-")
        uid.append(this.requestCounter++)
        uid.append("-")
        uid.append(methodValue)

        return uid.toString()
    }

    /**
     * 阻塞当前线程,以等待指定的请求信息已经得到响应了.
     *
     * @return 与请求相对应的响应信息
     */
    @Throws(CommunicationException::class)
    private fun waitForResponse(request: Request): Response<*> {

        val responseIdentifier = request.replyWith
        var response: Response<*>? = null
        val method = request.method

        log.debug("准备获取方法:{},请求符:{}的响应结果.", method, responseIdentifier)

        withLock(netLock) {
            if (this.isServerDown) {
                throw CommunicationException("目标地址连接已经中断")
            }

            //响应已经回来
            response = this.responses.remove(responseIdentifier)
            if (response != null) {
                return response!!
            }

            //使用中断来模拟线程间通知,当前线程等待,其它线程来通知自己
            val wt = WaitingThread(Thread.currentThread())
            this.waitingThreads[responseIdentifier] = wt
            while (!wt.hasBeenWokenUp()) {
                try {
                    netLockCondition.await()
                } catch (e: InterruptedException) {
                    //这里中断异常, 应该是响应已经拿到了,正常通知中断
                }

            }
            log.debug("当前线程已经被通知,获取了响应信息")

            this.waitingThreads.remove(responseIdentifier)
            response = this.responses.remove(responseIdentifier)
            log.debug("收到方法:{},请求符:{}的响应结果.", method, responseIdentifier)

            //如果没有响应结果,肯定是有什么问题了
            if (response == null) {
                log.debug("不知道什么原因,并没有收到响应结果")
                if (this.isServerDown) {
                    throw CommunicationException("目标节点连接已断开")
                }

                throw CommunicationException("目标节点没有断开,但却没有响应结果")
            }
        }
        return response!!
    }

    /** 处理收到的异步响应信息, 同时通知阻塞在响应上的线程  */
    private fun responseReceived(response: Response<*>) {
        withLock(netLock) {
            val inReplyTo = response.inReplyTo
            log.debug("收到请求信息:{} ", inReplyTo)

            this.responses[inReplyTo] = response

            //通知阻塞线程
            val waitingThread = this.waitingThreads[inReplyTo]
            if (waitingThread != null) {
                log.debug("请求上有线程被阻塞,通知它:{}", inReplyTo)
                waitingThread.wakeUp()
            }
        }
    }

    /** 指示相应的连接已经断开了,以方便作后续处理  */
    private fun connectionDown() {
        if (this.responses == null) {
            return
        }

        //本地存有响应信息,因此强行通知所有正在阻塞的线程
        withLock(netLock) {
            log.debug("连接已断开,强行通知阻塞线程")
            waitingThreads.values.forEach(Consumer<WaitingThread> { it.wakeUp() })
        }
    }

    /**
     * 使用目标请求方法以及相应的参数信息构建起相应的请求对象,同时生成相应的请求惟一标识符
     *
     * @param methodValue 请求方法标识
     * @param parameters  请求方法所需要参数信息
     */
    @Suppress("UNCHECKED_CAST")
    private fun createRequest(methodValue: MethodValue, vararg parameters: Any): Request {
        val responseIdentifier = this.createRequestIdentifier(methodValue)
        val request = Request()
        request.method = methodValue
        request.replyWith = responseIdentifier
        request.parameters = parameters as Array<Any>
        request.timeStamp = System.currentTimeMillis()

        return request
    }

    @Throws(CommunicationException::class)
    private fun sendAndWait(request: Request): Response<*> {
        val method = request.method

        log.debug("准备发送请求:{},数据:{}", method, request)
        this.send(request)

        val response = this.waitForResponse(request)
        log.debug("响应:{}已经收到:{} ", method, response)

        if (response.isFailureResponse) {
            throw CommunicationException(response.failureReason)
        }

        return response
    }

    @Suppress("UNCHECKED_CAST")
    private fun <R> requestAndResponse(methodValue: MethodValue, vararg parameters: Any, callback: (Response<*>) -> R = { it as R }): R {
        this.makeSocketAvailableAndWait()

        val request = this.createRequest(methodValue, *parameters)
        val response = sendAndWait(request)

        return callback(response)
    }

    @Suppress("UNCHECKED_CAST")
    override fun <T> notifyGetEntriesInterval(fromId: Id, toId: Id): List<Entry<T>> {
        return requestAndResponse(NOTIFY_GET_ENTRIES_INTERVAL, fromId, toId) { it.result as List<Entry<T>> }
    }

    override fun <T> notifyInsertEntriees(insertEntries: Map<Id, Entry<T>>) {
        requestAndResponse<Unit>(NOTIFY_INSERT_ENTRIES, insertEntries)
    }

    override fun start() {
        serviceStatus = ServiceStatus.RUNNING
    }

    override fun notifyFindValidNode(id: Id): RingNode? {
        return requestAndResponse(NOTIFY_FIND_VALID_NODE, id) { it.result?.let { selfOrProxy(it as RemoteNodeInfo) } }
    }

    override fun notifyFindValidNode4Get(id: Id): RingNode? {
        return requestAndResponse(NOTIFY_FIND_VALID_NODE_4_GET, id) { it.result?.let { selfOrProxy(it as RemoteNodeInfo) } }
    }

    /** 初始化目标节点的id值信息  */
    @Throws(CommunicationException::class)
    private fun initializeNodeID() {
        if (this.nodeId != ID_NOT_KNOWN) {
            return
        }

        requestAndResponse(GET_NODE_ID) { this.nodeId = it.result as Id }
    }

    private fun remotePreAndSuccessorsToLocal(value: RemotePreAndSuccessors): PreAndSuccessors {
        val pre = value.pre?.let { selfOrProxy(it) }
        val successorList = value.successorList.map { selfOrProxy(it) }
        return PreAndSuccessors(pre, successorList)
    }

    @Suppress("UNCHECKED_CAST")
    @Throws(CommunicationException::class)
    override fun notifyFindValidPreAndSuccessors(): PreAndSuccessors {
        return requestAndResponse(NOTIFY_FIND_VALID_PRE_AND_SUCCESSORS) {
            remotePreAndSuccessorsToLocal(it.result as RemotePreAndSuccessors)
        }
    }

    @Suppress("UNCHECKED_CAST")
    @Throws(CommunicationException::class)
    override fun notifyFindValidPreAndSuccessorsWithHint(potentialPredecessor: RingNode): PreAndSuccessors {
        val nodeInfoToSend = RemoteNodeInfo.of(potentialPredecessor)
        return requestAndResponse(NOTIFY_FIND_VALID_PRE_AND_SUCCESSORS_WITH_HINT, nodeInfoToSend) {
            remotePreAndSuccessorsToLocal(it.result as RemotePreAndSuccessors)
        }
    }

    @Throws(CommunicationException::class)
    override fun <T> insertEntry(key: Key, toInsert: T) {
        requestAndResponse<Unit>(INSERT_ENTRY, key, toInsert as Any)
    }

    @Throws(CommunicationException::class)
    override fun <T> notifyInsertReplicas(insertReplicas: Map<Id, Entry<T>>) {
        requestAndResponse<Unit>(NOTIFY_INSERT_REPLICAS, insertReplicas)
    }

    @Throws(CommunicationException::class)
    override fun leaveNetwork(predecessor: RingNode) {
        val nodeInfo = RemoteNodeInfo.of(predecessor)
        requestAndResponse<Unit>(LEAVE_NETWORK, nodeInfo)
    }

    @Throws(CommunicationException::class)
    override fun deleteEntry(entryToRemove: Key) {
        requestAndResponse<Unit>(DELETE_ENTRY, entryToRemove)
    }

    @Throws(CommunicationException::class)
    override fun notifyDeleteReplicas(sendingNode: Id, replicasToRemove: Set<Id>) {
        requestAndResponse<Unit>(NOTIFY_DELETE_REPLICAS, sendingNode, replicasToRemove)
    }

    @Suppress("UNCHECKED_CAST")
    @Throws(CommunicationException::class)
    override fun <T> getEntry(key: Key): T {
        return requestAndResponse(GET_ENTRY, key)
    }

    @Throws(CommunicationException::class)
    override fun notifyFindValidPreAndSuccessorsAndTransferDataWithHint(potentialPredecessor: RingNode): PreAndSucccessorsAndEntries {
        val nodeInfoToSend = RemoteNodeInfo.of(potentialPredecessor)

        return requestAndResponse(NOTIFY_FIND_VALID_PRE_AND_SUCCESSORS_AND_TRANSFER_DATA_WITH_HINT, nodeInfoToSend) {
            val result = it.result as RemotePreAndSucccessorsAndEntries

            val pre = result.pre?.let { selfOrProxy(it) }
            val successorList = result.successorList.mapTo(LinkedList()) { selfOrProxy(it) }

            return@requestAndResponse PreAndSucccessorsAndEntries(pre, successorList, result.entries)
        }
    }

    @Throws(CommunicationException::class)
    override fun notifyChangeStoreToReplicas(potentialPredecessor: RingNode) {
        val nodeInfoToSend = RemoteNodeInfo.of(potentialPredecessor)
        requestAndResponse<Unit>(NOTIFY_CHANGE_STORE_TO_REPLICAS, nodeInfoToSend)
    }

    override fun <T> notifyPreInsertEntry(key: Key, toInsert: T, invokingNodeIdList: List<Id>) {
        withinDepth(invokingNodeIdList) { requestAndResponse<Unit>(NOTIFY_PRE_INSERT_ENTRY, key, toInsert as Any, invokingNodeIdList) }
    }

    override fun notifyPreDeleteEntry(entryToRemove: Key, invokingNodeIdList: List<Id>) {
        withinDepth(invokingNodeIdList) { requestAndResponse<Unit>(NOTIFY_PRE_DELETE_ENTRY, entryToRemove, invokingNodeIdList) }
    }

    override fun <T> notifyPreGetEntry(key: Key, invokingNodeIdList: List<Id>): T? {
        return withinDepth(invokingNodeIdList) { requestAndResponse(NOTIFY_PRE_GET_ENTRY, key, invokingNodeIdList) }
    }

    @Transient
    private var stringRepresentation: String? = null

    override fun toString(): String {
        if (this.stringRepresentation == null) {
            stringRepresentation = "Proxy{id:$nodeId,url:$nodeUrl,local:$urlOfLocalNode}"
        }
        return this.stringRepresentation!!
    }

    /** 描述一个存在等待数据响应的线程信息  */
    private class WaitingThread(private val thread: Thread) {

        /** 是否已经被唤醒  */
        private var hasBeenWokenUp = false

        /** 此线程是否已经被唤醒  */
        internal fun hasBeenWokenUp(): Boolean {
            return this.hasBeenWokenUp
        }

        /** 唤醒一个正在等待响应数据的线程(通过中断来唤醒)  */
        internal fun wakeUp() {
            this.hasBeenWokenUp = true
            this.thread.interrupt()
        }

        override fun toString(): String {
            return this.thread.toString() + ": Waiting? " + !this.hasBeenWokenUp()
        }
    }

    //---------------------------- 实现网络连接,断开过程 start ------------------------------//


    private fun changeChannel(newChannel: Channel?) {
        log.debug("切换连接通道,客户端:{},旧通道:{},新通道:{}", urlOfLocalNode, channel, newChannel)
        Optional.ofNullable(channel).ifPresent { it.close() }

        channel = newChannel
        isCurrentAvaible = newChannel != null

        lockChannelAndSignal()
    }

    internal fun connect() {
        val errorNum = AtomicInteger()
        val currentErrorNumMax = ERROR_NUM_MIN + (Math.random() * (ERROR_NUM_MAX - ERROR_NUM_MIN)).toInt()

        doConnect(errorNum, currentErrorNumMax)
    }

    private fun doConnect(errorNum: AtomicInteger, currentErrorNumMax: Int) {
        val channelFuture = bootstrap!!.connect()
        channelFuture.addListener { future ->
            if (future.isSuccess) {
                val channel = channelFuture.channel()
                log.debug("连接了:{}次, 建立起链接,客户端:{}", errorNum.get() + 1, urlOfLocalNode)
                changeChannel(channel)
                return@addListener
            }

            changeChannel(null)

            val cause = channelFuture.cause()
            log.debug("连接失败:{},当前失败次数:{},客户端:{}", cause.message, errorNum.get(), urlOfLocalNode)
            log.debug("异常堆栈信息:", cause)

            val currentErrorNum = errorNum.incrementAndGet()
            if (currentErrorNum >= currentErrorNumMax) {
                log.error("连接次数超过次数:{},客户端:{}.是否重连:{}", currentErrorNum, urlOfLocalNode, !firstConnect.get())
                optiDisconnect()
                return@addListener
            }

            val delay = if (cause is ConnectTimeoutException) 0 else CONNECTION_RETRY_TIME
            asyncGroup.schedule({ doConnect(errorNum, currentErrorNumMax) }, delay, TimeUnit.MILLISECONDS)
        }
    }

    /** 保证相应的网络端口已经被打开  */
    @Throws(CommunicationException::class)
    private fun makeSocketAvailableAndWait() {
        if (this.isServerDown) {
            throw CommunicationException("相应的网络连接从: " + this.urlOfLocalNode + " 到 " + this.nodeUrl + " 已经断开")
        }

        val firstConncted = firstConnect.get()

        tryMakeSocketAvailable()

        //如果仍未可用,则尝试等待特定时间
        if (!firstConncted && !isCurrentAvaible) {
            lockChannelAndAwait(CONNECT_TIMEOUT)
        }

        if (!isCurrentAvaible) {
            val message = StringUtils.format("当前连接还没有建立起来(且已超时),请更换节点重试.当前节点:{},目标节点:{}", urlOfLocalNode, nodeUrl)
            throw CommunicationException(message)
        }
    }

    /** 尝试让当前网络端口可用,如果还未初始化连接,则尝试建立连接  */
    private fun tryMakeSocketAvailable() {
        if (this.isServerDown) {
            return
        }

        if (firstConnect.getAndSet(false)) {
            init()
            start()
            connect()
            if (!isCurrentAvaible) {
                lockChannelAndAwait(CONNECT_TIMEOUT)
            }
        }
    }

    /** 主动断开连接  */
    private fun optiDisconnect() {
        disconnect()

        proxyListenerList.forEach { t -> ExceptionUtils.doActionLogE(log) { t.serverDown(this) } }
    }

    /** 断开当前代理,即当前代理连接已不再使用,被动式  */
    override fun disconnect() {
        //已经断开过,则直接返回,可能是循环调用
        if (this.isServerDown) {
            return
        }

        serviceStatus = ServiceStatus.STOPPED

        log.info("准备断开代理连接,从:{}到{}", this.urlOfLocalNode, this.nodeUrl)
        this.isServerDown = true
        this.isCurrentAvaible = false

        proxyListenerList.forEach { t -> ExceptionUtils.doActionLogE(log) { t.serverDown(this) } }

        synchronized(proxyMap) {
            proxyMap -= generateProxyKey(this.urlOfLocalNode, this.nodeUrl)
        }

        this.connectionDown()
    }

    companion object {
        private val log = loggerFor<SocketProxy>()
        private val ID_NOT_KNOWN = Id(Id.MAX.id, "proxy哨兵节点")

        private const val ERROR_NUM_MIN = 5
        private const val ERROR_NUM_MAX = 10

        private const val CONNECT_TIMEOUT: Long = 5000
        private const val CONNECTION_RETRY_TIME = CONNECT_TIMEOUT - 2000

        /** 全局连接缓存  */
        private val proxyMap = HashMap<String, SocketProxy>()
        /** 用于在进行远程访问时的异步任务线程池  */
        internal val asyncGroup: EventLoopGroup = DefaultEventLoopGroup(0, ThreadFactoryUtils.build("proxyAsyncThread-%d"))

        /**
         * 创建一个不确定的网络连接，需要从远处的节点处获取相应的节点信息
         *
         * @param urlOfLocalNode 当前客户端地址信息
         * @param url            目标地址
         */
        @Throws(CommunicationException::class)
        fun createUnknown(urlOfLocalNode: Url, url: Url): SocketProxy {
            val p = createKnown(urlOfLocalNode, url, ID_NOT_KNOWN)
            return p.apply { initializeNodeID() }
        }

        /** 由一个远程的节点信息引用创建起相应的代理对象 */
        fun createKnown(urlOfLocalNode: Url, nodeInfo: RemoteNodeInfo): SocketProxy {
            val value = createKnown(urlOfLocalNode, nodeInfo.nodeUrl, nodeInfo.nodeId)
            value.notifiable = nodeInfo.notifiable

            return value
        }

        /**
         * 创建一个确定的网络连接,并且使用指定的id表示目标id信息(即不需要初次连接去获取id信息)
         *
         * @param url            目标节点地址
         * @param urlOfLocalNode 本地节点地址
         * @param nodeID         目标节点id信息
         */
        fun createKnown(urlOfLocalNode: Url, url: Url, nodeID: Id): SocketProxy {
            synchronized(proxyMap) {
                val proxyKey = generateProxyKey(urlOfLocalNode, url)
                var proxy: SocketProxy? = proxyMap[proxyKey]
                if (proxy != null) {
                    return proxy
                }

                proxy = SocketProxy(urlOfLocalNode, url, nodeID)

                proxyMap[proxyKey] = proxy
                return proxy
            }
        }

        /** 产生一个双方连接的缓存key,以减少重复创建  */
        private fun generateProxyKey(localURL: Url, remoteURL: Url) = "$localURL->$remoteURL"
    }

    //---------------------------- 实现网络连接,断开过程 end ------------------------------//

}