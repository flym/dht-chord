package io.iflym.dht.node.proxy

import io.iflym.dht.model.Id
import io.iflym.dht.model.Url
import io.iflym.dht.net.ProxyListener
import io.iflym.dht.node.AbstractRingNode
import io.iflym.dht.node.ProxyNode

/**
 * 代理对象用于代理一个对目标网络节点的访问,即看起来是直接操作目标对象一样.
 * 目标对象使用chord来表示,而proxy就是对chord的代理,以用于引用相应的目标chord. 2个节点之间即通过proxy来进行引用处理.
 * proxy能够通过访问目标chord开放出来的endpoint, 这样形成一个client/server的结构,以进行实际通信.
 *
 *
 * chord支持多种不同的协议,对于实现者,需要同时实现endpoint和proxy,即实现server和client
 *
 *
 * proxy之所以继承自node,主要目的是让proxy看起来就是一个实际的node,对于使用者来说,不用关注到底下层是一个实际的存储还是一个代理.
 * 一般来说,使用者只需要使用其node对象来进行数据访问即可.
 *
 * @author flym
 */
abstract class AbstractProxyNode protected constructor(nodeId: Id, override val nodeUrl: Url) : AbstractRingNode(), ProxyNode {
    override var nodeId: Id = nodeId
        protected set

    protected val proxyListenerList: MutableList<ProxyListener> = mutableListOf()

    override fun addProxyListener(proxyListener: ProxyListener) {
        proxyListenerList += proxyListener
    }
}
