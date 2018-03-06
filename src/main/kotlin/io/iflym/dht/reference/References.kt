package io.iflym.dht.reference

import io.iflym.dht.model.Id
import io.iflym.dht.net.ProxyListener
import io.iflym.dht.node.ProxyNode
import io.iflym.dht.node.RingNode
import java.util.*

/**
 * 用于描述对网络节点的其它节点引用信息
 * Created by flym on 7/13/2017.
 */
interface References : ProxyListener {

    /** 获取此引用所对应节点的id  */
    val nodeId: Id

    /** 一个引用表的索引表  */
    fun fingerTable(): FingerTable

    /** 一个引用表的后缀表  */
    fun successorList(): SuccessorList

    /** 一个引用表的前缀(如果有)  */
    fun predecessor(): ProxyNode?

    /** 添加一堆后缀节点列表到引用表 */
    fun addReferences(paramSuccessorList: List<RingNode>) {
        paramSuccessorList.asSequence()
                .filterNot { it.nodeId != nodeId }
                .filter { it is ProxyNode }
                .map { it as ProxyNode }
                .filterNot { containsReference(it) }
                .forEach { addReference(it) }

    }

    /** 添加新的引用节点到引用表中  */
    fun addReference(newReference: ProxyNode)

    /** 移除相应的引用节点  */
    fun removeReference(oldReference: ProxyNode)

    /** 是否包括相应的引用节点  */
    fun containsReference(newReference: ProxyNode): Boolean

    /** 获取有效的当前最接近指定key的前缀节点  */
    fun getValidClosestPrecedingNode(key: Id): RingNode?

    /** 断开并且清除相应的引用  */
    fun clearAndDisconnect()

    fun findSuccessorWithinReferences(id: Id): RingNode?

    fun findSuccessorWithinReferences4Get(id: Id): RingNode?

    /**
     * 关掉一个可能在当前引用表中并不再需要的代理节点
     * 因为不再需要,因此之前的网络连接代理就不再需要,则关闭之(主要是一些连接端口, 异步线程)
     *
     * @param removedReference 已经调用过一次移除操作的引用节点
     */
    fun disconnectIfUnreferenced(removedReference: ProxyNode) {
        Objects.requireNonNull<Any>(removedReference, "断开已移除的引用时相应引用不能为null")

        if (!this.containsReference(removedReference)) {
            removedReference.disconnect()
        }
    }

    override fun serverDown(proxy: ProxyNode) {
        //以下的if判断必须存在, 以避免反复双向互相调用的问题(因为remove也会间接地调用proxy.disconnect)
        if (this.containsReference(proxy))
            this.removeReference(proxy)
    }
}
