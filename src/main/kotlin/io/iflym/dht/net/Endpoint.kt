package io.iflym.dht.net

import io.iflym.dht.model.Url
import io.iflym.dht.node.persistent.PersistentNode
import io.iflym.dht.node.proxy.socket.SocketEndpoint
import io.iflym.dht.node.proxy.socket.data.MethodValue
import org.slf4j.LoggerFactory
import java.util.*

/**
 * 一个node的对外提供服务服务端, 通过代理对node的直接访问请求,对外提供服务.每个在chord网络的node都必须要有一个endpoint,以作为对外联系的端点.
 * 对外提供服务可以使用不同的处理协议, 当前类提供一系列的抽象接口以方便子类来实现.针对于所有的协议来说,使用者都可以通过当前类提供的[.createEndpoint]
 * 来获取相应的端点服务,而不是管是哪一种实现.
 *
 * 每个endpoint都有3种状态值,以表示当前处于不同的阶段
 * STARTED
 * LISTENING
 * ACCEPT_ENTRIES
 *
 *
 * STARTED状态表示服务端已经初始化好了,但还不能接收来自chord网络的请求信息.一个endpoint在刚创建时即为此状态值.
 *
 *
 * LISTENING状态表示可以接收一些来自于chord网络以更新相应的引用表或者前后缀节点信息的请求信息.
 *
 *
 * ACCEPT_ENTRIES状态表示可以收到来自chord网络关于数据存储,复制,删除数据的请求信息.
 *
 * @author flym
 */
abstract class Endpoint protected constructor(node: PersistentNode, url: Url) {

    /** 当前状态值  */
    private var state = -1

    /** 服务端对外提供服务地址  */
    var url: Url
        protected set

    /** 服务端所对应的node节点  */
    var node: PersistentNode
        protected set

    /** 所有对服务端状态变更有兴趣的监听器,以待状态变化时进行相应的处理  */
    private val listeners = HashSet<EndpointStateListener>()

    init {
        this.node = node
        this.url = url
        this.state = STARTED
    }

    /** 注册状态监听器  */
    fun register(listener: EndpointStateListener) {
        this.listeners.add(listener)
    }

    /** 移除不再需要的状态监听器(一般是此监听器已不再工作,如断开连接等)  */
    fun deregister(listener: EndpointStateListener) {
        this.listeners.remove(listener)
    }

    /**
     * 通知监听器当前服务端状态发生了变化
     *
     * @param state 相应的状态值
     */
    protected fun notify(state: Int) {
        synchronized(this.listeners) {
            listeners.forEach { t -> t.notify(state) }
        }
    }

    protected fun setState(state: Int) {
        this.state = state
        this.notify(state)
    }

    fun getState() = state

    /** 准备开始监听请求并打开对外连接服务  */
    fun listen() {
        setState(LISTENING)
        this.openConnections()
    }

    /** 打开连接服务(如监听端口,接收相应请求)  */
    protected abstract fun openConnections()

    /** 准备开始接收数据操作类请求  */
    fun acceptEntries() {
        setState(ACCEPT_ENTRIES)
        this.entriesAcceptable()
    }

    /** 通知子类可以接收数据类请求了  */
    protected abstract fun entriesAcceptable()

    /** 断开所有连接,并且停止并删除当前对外服务信息  */
    fun disconnect() {
        log.info("准备断开服务端连接.")
        setState(STARTED)

        this.doDisconnect()
        synchronized(endpoints) {
            endpoints.remove(this.node.nodeUrl)
        }
    }

    /** 执行实际的断开网络操作  */
    protected abstract fun doDisconnect()

    override fun toString(): String {
        val buffer = StringBuilder()
        buffer.append("[Endpoint for ")
        buffer.append(this.node)
        buffer.append(" with URL ")
        buffer.append(this.url)
        return buffer.toString()
    }

    companion object {
        val log = LoggerFactory.getLogger(Endpoint::class.java)!!

        /** 全局对象用于引用每个对外网络所提供的endpoint服务.每个node通过它的url映射到相应的endpoint  */
        val endpoints: MutableMap<Url, Endpoint> = mutableMapOf()

        const val STARTED = 0
        const val LISTENING = 1
        const val ACCEPT_ENTRIES = 2
        const val DISCONNECTED = 3

        /** 哪些请求方法必须在ACCEPT_ENTRIES状态下才可调用.  */
        val METHODS_ALLOWED_IN_ACCEPT_ENTRIES: List<MethodValue> = listOf(MethodValue.INSERT_ENTRY, MethodValue.DELETE_ENTRY, MethodValue.GET_ENTRY)

        /**
         * 为相应的节点通过其对外服务地址创建相应的对外端点信息.
         * 每个对外地址仅能创建一个服务端,如果一个服务端已经断开连接,那么必须重新创建相应的服务才可以工作
         *
         * @param node 对外服务所代理的节点信息
         * @param url  对外暴露地服务地址
         */
        fun createEndpoint(node: PersistentNode, url: Url): Endpoint {
            Objects.requireNonNull<Any>(url, "对外服务地址不能为空")

            synchronized(endpoints) {
                if (endpoints.containsKey(url)) {
                    throw RuntimeException("相应对外地址:" + url + "的服务已经存在了")
                }
                val endpoint: Endpoint

                val protocol = url.protocol
                endpoint = when (protocol) {
                    Url.PROTOCOL_SOCKET -> SocketEndpoint(node, url)
                    else -> throw IllegalArgumentException("不支持的协议:" + protocol)
                }

                endpoints.put(url, endpoint)

                return endpoint
            }
        }

        /** 通过对外地址获取相应的服务信息  */
        fun getEndpoint(url: Url): Endpoint {
            synchronized(endpoints) {
                return endpoints[url]!!
            }
        }
    }

}