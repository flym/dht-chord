package io.iflym.dht.node.access

import io.iflym.dht.model.Id
import io.iflym.dht.node.ProxyNode
import io.iflym.dht.node.RingNode
import io.iflym.dht.reference.AbstractReferences
import io.iflym.dht.reference.FingerTable
import io.iflym.dht.reference.References
import io.iflym.dht.reference.SuccessorList

/**
 * 用于维护单个网络节点用于访问部外节点时的一些引用信息,包括前缀,后缀,索引表等信息.
 * 对于一个chord网络节点来说, 相应的关系类似于chord->chord->reference(pre,successor,fingerTable)这样的一个关系
 *
 * @author flym
 */
class AccessReferences(node: RingNode) : AbstractReferences(node), References {

    /** 索引表  */
    private val fingerTable: AccessFingerTable = AccessFingerTable(nodeId, this)

    /** 当前节点的后缀表  */
    private val successorList: AccessSuccessorList = AccessSuccessorList(node, NUMBER_OF_SUCCESSORS, this)

    override fun fingerTable(): FingerTable {
        return fingerTable
    }

    override fun successorList(): SuccessorList {
        return successorList
    }

    override fun predecessor(): ProxyNode? {
        //没有前缀
        return null
    }

    override fun removeDoPredecessor(oldReference: ProxyNode) {
        //没前缀,不做事
        //nothing to do
    }

    override fun missedGetValidClosestPrecedingNode(key: Id): RingNode? {
        var result = handleWithFingerTable<RingNode?>({ t ->
            val list = t.getFirstFingerTableEntries(1)
            if (list.isEmpty()) null else list.get(0)
        })

        if (result != null)
            return result

        result = handleWithSuccessorList({ it.directSuccessor })

        return result
    }
}