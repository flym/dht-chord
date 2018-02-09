package io.iflym.dht.node

import io.iflym.dht.exception.CommunicationException
import io.iflym.dht.exception.ServiceException
import io.iflym.dht.model.Id
import io.iflym.dht.model.Key
import io.iflym.dht.model.Url
import io.iflym.dht.net.ProxyListener
import io.iflym.dht.node.proxy.socket.SocketProxy
import io.iflym.dht.util.Asserts
import java.util.*


/**
 * 用于代理一个实际节点的代理对象
 * 在环上的节点之间，用于引用其它的节点都使用代理节点来表示，即在本地，可以看到其它节点，但看到的是一个引用关系
 *
 * @author flym
 * */
interface ProxyNode : RingNode {
    /** 当前服务是否已挂掉,即不再可访问 */
    val isServerDown: Boolean

    /**
     * 当前是否是活跃的(即在当前时刻是可以访问的)
     * 此方法带有相应的检测机制,如果相应的代码类仍未初始化,则会尝试进行初始化,即会返回一个当前代码在当前点确实是一个确实的状态(能连上/不可连接之类)
     */
    val isCurrentAlive: Boolean

    /** 代理节点支持事件监听回调 */
    fun addProxyListener(proxyListener: ProxyListener)

    override fun findValidNode(id: Id): RingNode {
        throw ServiceException("代理不支持此操作。findValidNode")
    }

    override fun findValidNode4Get(id: Id): RingNode {
        throw ServiceException("代理不支持此操作。findValidNode4Get")
    }

    companion object {
        /**
         * 从当前地址创建一个指向目标地址的代理连接对象. 2个地址必须使用相同的协议信息,并且此协议是已知的.
         *
         * @param sourceUrl      原访问节点,不一定是存储节点,仅表示当前访问位置
         * @param destinationUrl 目标地址,必须是一个存储节点所对应的endpoint地址,即server地址
         */
        @Throws(CommunicationException::class)
        fun of(sourceUrl: Url, destinationUrl: Url): ProxyNode {
            Objects.requireNonNull<Any>(sourceUrl, "源地址不能为null")
            Objects.requireNonNull<Any>(destinationUrl, "目标地址不能为null")

            Asserts.assertTrue(sourceUrl != destinationUrl, "源地址和目标地址不能相同")

            val protocol = destinationUrl.protocol
            return when (protocol) {
                Url.PROTOCOL_SOCKET -> SocketProxy.createUnknown(sourceUrl, destinationUrl)
                else -> throw RuntimeException("不支持的协议:" + protocol)
            }
        }
    }
}