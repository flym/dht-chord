package io.iflym.dht.node.proxy.socket

import com.google.common.util.concurrent.ThreadFactoryBuilder
import io.iflym.dht.exception.CommunicationException
import io.iflym.dht.model.Entry
import io.iflym.dht.model.Id
import io.iflym.dht.model.Key
import io.iflym.dht.net.Endpoint
import io.iflym.dht.net.EndpointStateListener
import io.iflym.dht.node.RingNode
import io.iflym.dht.node.proxy.socket.data.*
import io.netty.channel.socket.SocketChannel
import io.netty.util.AttributeKey
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

/**
 * 一次调用请求的执行线程,用于执行实际的请求并输出响应信息
 *
 * @author flym
 */
class InvocationChannel(
        /** 反向引用相应的对外server  */
        val endpoint: SocketEndpoint,
        /** 相应的连接通道  */
        private val socketChannel: SocketChannel) : EndpointStateListener {

    /** 执行相应处理请求逻辑所在的本地节点信息  */
    private val node: RingNode

    /** 当前连接是否仍有效  */
    var isConnected = true

    /**
     * 描述当前对外server的处理状态值,如开始,启动中,监听,关闭等
     *
     * @see Endpoint
     */
    private var state: Int = 0

    /** 需要等待执行许可条件的线程集合,用于记录这些还不满足执行条件的线程信息  */
    private val waitingThreads = HashSet<Thread>()

    private val lock = java.lang.Object()

    fun <T> writeAndFlush(response: Response<T>) {
        socketChannel.writeAndFlush(response)
    }

    fun createNewRequestHandler(request: Request): RequestInvocation {
        return RequestInvocation(this, request)
    }

    /**
     * 执行实际的请求方法
     *
     * @param methodValue 请求方法定义符,用于定位执行方法
     * @param parameters  相应的请求方法调用传递的参数信息
     * @return 执行结果, 如果方法本身为void, 则返回null
     */
    @Suppress("UNCHECKED_CAST")
    @Throws(Exception::class)
    fun invokeMethod(methodValue: MethodValue, vararg parameters: Any): Any? {
        //某些操作需要在特定状态下执行,如果条件不允许,则阻塞
        this.waitForMethod(methodValue)

        if (!this.isConnected) {
            throw CommunicationException("连接已经被关闭了,不能再执行请求方法")
        }

        var result: Any? = null
        log.debug("准备执行方法:{},参数信息为:{}", methodValue, parameters)

        when (methodValue) {
        //后缀
            MethodValue.NOTIFY_FIND_VALID_NODE -> {
                val node = this.node.notifyFindValidNode(parameters[0] as Id)
                result = node?.let { RemoteNodeInfo.of(it) }
            }
        //查找目的_后缀
            MethodValue.NOTIFY_FIND_VALID_NODE_4_GET -> {
                val node = this.node.notifyFindValidNode4Get(parameters[0] as Id)
                result = node?.let { RemoteNodeInfo.of(it) }
            }
        //查找当前节点id
            MethodValue.GET_NODE_ID -> {
                result = this.node.nodeId
            }
        //存储数据
            MethodValue.INSERT_ENTRY -> {
                this.node.insertEntry(parameters[0] as Key, parameters[1])
            }
        //存储副本
            MethodValue.NOTIFY_INSERT_REPLICAS -> {
                this.node.notifyInsertReplicas(parameters[0] as Map<Id, Entry<Any>>)
            }
        //前缀离开网络
            MethodValue.LEAVE_NETWORK -> {
                val nodeInfo = parameters[0] as RemoteNodeInfo
                val predecessor = SocketProxy.createKnown(this.node.nodeUrl, nodeInfo)
                this.node.leaveNetwork(predecessor)
            }
        //请求节点信息
            MethodValue.NOTIFY_FIND_VALID_PRE_AND_SUCCESSORS -> {
                val value = this.node.notifyFindValidPreAndSuccessors()
                result = RemotePreAndSuccessors.of(value)
            }

        //请求节点信息(带前缀暗示)
            MethodValue.NOTIFY_FIND_VALID_PRE_AND_SUCCESSORS_WITH_HINT -> {
                val nodeInfo = parameters[0] as RemoteNodeInfo
                val potentialPredecessor = SocketProxy.createKnown(this.node.nodeUrl, nodeInfo)

                val value = this.node.notifyFindValidPreAndSuccessorsWithHint(potentialPredecessor)
                result = RemotePreAndSuccessors.of(value)
            }
        //请求节点及迁移数据
            MethodValue.NOTIFY_FIND_VALID_PRE_AND_SUCCESSORS_AND_TRANSFER_DATA_WITH_HINT -> {
                val nodeInfo = parameters[0] as RemoteNodeInfo
                val potentialPredecessor = SocketProxy.createKnown(this.node.nodeUrl, nodeInfo)

                val value = this.node.notifyFindValidPreAndSuccessorsAndTransferDataWithHint(potentialPredecessor)
                result = RemotePreAndSucccessorsAndEntries.of(value)
            }
        //通知数据存储变副本
            MethodValue.NOTIFY_CHANGE_STORE_TO_REPLICAS -> {
                val nodeInfo = parameters[0] as RemoteNodeInfo
                val potentialPredecessor = SocketProxy.createKnown(this.node.nodeUrl, nodeInfo)

                this.node.notifyChangeStoreToReplicas(potentialPredecessor)
            }
        //删除数据
            MethodValue.DELETE_ENTRY -> {
                this.node.deleteEntry(parameters[0] as Key)
            }
        //删除副本
            MethodValue.NOTIFY_DELETE_REPLICAS -> {
                this.node.notifyDeleteReplicas(parameters[0] as Id, parameters[1] as Set<Id>)
            }
        //查询数据
            MethodValue.GET_ENTRY -> {
                result = this.node.getEntry<Any>(parameters[0] as Key)
            }

        //范围内获取数据
            MethodValue.NOTIFY_GET_ENTRIES_INTERVAL -> {
                result = node.notifyGetEntriesInterval<Any>(parameters[0] as Id, parameters[1] as Id)
            }

        //插入多个数据
            MethodValue.NOTIFY_INSERT_ENTRIES -> {
                node.notifyInsertEntriees(parameters[0] as Map<Id, Entry<Any>>)
            }

        //通知前缀插入数据
            MethodValue.NOTIFY_PRE_INSERT_ENTRY -> {
                node.notifyPreInsertEntry(parameters[0] as Key, parameters[1], parameters[2] as List<Id>)
            }

        //通知前缀删除数据
            MethodValue.NOTIFY_PRE_DELETE_ENTRY -> {
                node.notifyPreDeleteEntry(parameters[0] as Key, parameters[1] as List<Id>)
            }

        //通知前缀获取数据
            MethodValue.NOTIFY_PRE_GET_ENTRY -> {
                result = node.notifyPreGetEntry(parameters[0] as Key, parameters[1] as List<Id>)
            }
        }

        return result
    }

    /**
     * 等待执行许可
     * 某些方法需要在特定的连接状态下才可以执行,因此这里通过相应的状态判断其是否可执行,如果不能执行,则阻塞相应的线程.
     * 直到相应的状态变更通知被收到
     *
     * @param methodValue 当前要执行的方法
     * @see [notify]
     */
    private fun waitForMethod(methodValue: MethodValue) {
        synchronized(this.lock) {
            //仅阻塞需要在可接收数据的状态下执行的方法,其它与状态无关的方法可直接放行
            //同时,如果连接不可用,则直接放过,因为无意义
            val methodInAcceptStatus = Endpoint.METHODS_ALLOWED_IN_ACCEPT_ENTRIES.contains(methodValue)
            while (this.state != Endpoint.ACCEPT_ENTRIES && this.isConnected && methodInAcceptStatus) {
                val currentThread = Thread.currentThread()
                log.debug("当前线程:{}被阻塞,等待执行方法:{}", currentThread, methodValue)
                this.waitingThreads.add(currentThread)
                try {
                    this.lock.wait()
                } catch (ignore: InterruptedException) {
                    // do nothing
                }

                log.debug("阻塞线程:{},执行方法:{}被唤醒", currentThread, methodValue)
                this.waitingThreads.remove(currentThread)
            }
        }

        log.debug("阻塞方法:{}已经被放行,可以执行了!", methodValue)
    }


    /** 断开处理线程,置相应断开标记.即不再处理此请求,同时关闭相应处理流信息  */
    fun disconnect() {
        log.info("准备断开连接.{}", isConnected)

        if (!this.isConnected) {
            log.debug("连接已断开,直接返回")
            return
        }

        //因为有些请求处理还在等待队列中,这里将其唤醒.但是先置相应的标记,以让这些唤醒的线程跳过实际执行的流程.即唤醒之后,就不再执行请求处理
        synchronized(this.lock) {
            this.isConnected = false
            this.lock.notifyAll()
        }
    }

    override fun notify(newState: Int) {
        log.debug("收到新的状态通知:{}", newState)

        this.state = newState
        //通知之前被阻塞的线程
        synchronized(this.lock) {
            this.lock.notifyAll()
        }
    }

    companion object {
        private val log = LoggerFactory.getLogger(InvocationChannel::class.java)!!
        val ATTR_INVOCATION = AttributeKey.valueOf<InvocationChannel>("channel-invocation")!!

        /** 配置属性名-线程池核心数  */
        private const val CORE_POOL_SIZE_PROPERTY_NAME = "requestHandle.corepoolsize"

        /** 配置属性名-线程池最大数  */
        private const val MAX_POOL_SIZE_PROPERTY_NAME = "requestHandle.maxpoolsize"

        /** 配置属性名-线程池线程存活时间  */
        private const val KEEP_ALIVE_TIME_PROPERTY_NAME = "requestHandle.keepalivetime"

        /** 线程池核心数  */
        private val CORE_POOL_SIZE = Integer.parseInt(System.getProperty(CORE_POOL_SIZE_PROPERTY_NAME))

        /** 线程池最大数  */
        private val MAX_POOL_SIZE = Integer.parseInt(System.getProperty(MAX_POOL_SIZE_PROPERTY_NAME))

        /** 线程池线程存活时间  */
        private val KEEP_ALIVE_TIME = Integer.parseInt(System.getProperty(KEEP_ALIVE_TIME_PROPERTY_NAME))

        /** 创建单个节点对外接口的请求处理线程池,所有实际的请求(按次)执行逻辑均由此线程池来执行  */
        fun createInvocationThreadPool(): ThreadPoolExecutor {
            val threadFactory = ThreadFactoryBuilder().setNameFormat("InvocationChannel-%d").build()
            return ThreadPoolExecutor(CORE_POOL_SIZE, MAX_POOL_SIZE, KEEP_ALIVE_TIME.toLong(), TimeUnit.SECONDS,
                    LinkedBlockingQueue(), threadFactory)
        }
    }

    init {
        this.state = endpoint.getState()
        this.node = endpoint.node
    }
}