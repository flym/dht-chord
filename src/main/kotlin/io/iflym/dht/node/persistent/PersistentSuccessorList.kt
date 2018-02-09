package io.iflym.dht.node.persistent

import io.iflym.dht.exception.CommunicationException
import io.iflym.dht.model.Entry
import io.iflym.dht.model.Id
import io.iflym.dht.node.ProxyNode
import io.iflym.dht.node.RingNode
import io.iflym.dht.reference.AbstractSuccessorList

/**
 * 维护一个当前节点的后缀节点信息
 *
 * @author flym
 */
class PersistentSuccessorList(node: RingNode, numberOfEntries: Int, references: PersistentReferences) : AbstractSuccessorList(node, numberOfEntries, references) {
    override fun mayCopyReplicas(successorProxy: ProxyNode) {
        //将需要复制的副本传递给新的后缀节点,以进行副本复制,复制范围即当前前缀至当前之间的数据,即自己本来存储的数据(非复制)
        val fromID: Id
        val predecessor = this.references.predecessor()
        fromID = if (predecessor != null) {
            predecessor.nodeId
        } else {
            val precedingNode = this.references.getValidClosestPrecedingNode(this.localID)
            precedingNode?.nodeId ?: this.localID
        }

        val toID = this.localID
        val entriesToReplicate = node.notifyGetEntriesInterval<Any>(fromID, toID)
        try {
            val copyedEntries = mutableMapOf<Id, Entry<Any>>()
            entriesToReplicate.forEach { copyedEntries[it.key.id] = it }
            successorProxy.notifyInsertReplicas(copyedEntries)
        } catch (e: CommunicationException) {
            this.logger.warn("复制副本数据至新后缀节点时失败.新后缀节点:{},失败原因:{}", successorProxy, e.message, e)
        }
    }

    override fun mayRemoveReplicas(successorProxy: ProxyNode) {
        try {
            //因为相应的前缀已经移除,因为当前节点的所有副本都需要被移除掉
            successorProxy.notifyDeleteReplicas(this.localID, emptySet())
        } catch (e: CommunicationException) {
            this.logger.warn("超限后缀节点移除副本数据时失败.待移除后缀节点:{},失败原因:{}", successorProxy, e.message, e)
        }
    }
}