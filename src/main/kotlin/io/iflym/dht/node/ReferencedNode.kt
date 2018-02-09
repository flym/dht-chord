package io.iflym.dht.node

import io.iflym.dht.exception.ServiceException
import io.iflym.dht.reference.References


/**
 * 描述具有可以引用其它节点的引用表的节点， 通过引用表来访问其它节点
 * @author flym
 */
interface ReferencedNode : RingNode {
    /** 引用表信息 */
    val references: References

    /** 添加引用 */
    fun addReference(newReference: io.iflym.dht.node.ProxyNode) = references.addReference(newReference)

    fun leaveMayNotifyOthers()

    @Throws(ServiceException::class)
    fun processRefsInJoin(bootstrapNode: ProxyNode, newSuccessor: RingNode)
}