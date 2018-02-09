package io.iflym.dht.node.persistent

import io.iflym.dht.exception.CommunicationException
import io.iflym.dht.model.Entry
import io.iflym.dht.model.Id
import io.iflym.dht.node.ProxyNode
import io.iflym.dht.node.RingNode
import io.iflym.dht.reference.AbstractReferences
import io.iflym.dht.reference.FingerTable
import io.iflym.dht.reference.References
import io.iflym.dht.reference.SuccessorList
import java.util.*

/**
 * 用于维护单个网络节点用于访问部外节点时的一些引用信息,包括前缀,后缀,索引表等信息.
 * 对于一个chord网络节点来说, 相应的关系类似于chord->chord->reference(pre,successor,fingerTable)这样的一个关系
 *
 * @author flym
 */
class PersistentReferences(node: RingNode) : AbstractReferences(node), References {
    /** 索引表  */
    private val fingerTable: PersistentFingerTable = PersistentFingerTable(nodeId, this)

    /** 当前节点的后缀表  */
    private val successorList: PersistentSuccessorList = PersistentSuccessorList(node, NUMBER_OF_SUCCESSORS, this)

    /** 当前节点前缀  */
    private var predecessor: ProxyNode? = null

    override fun fingerTable(): FingerTable {
        return fingerTable
    }

    override fun successorList(): SuccessorList {
        return successorList
    }

    override fun predecessor(): io.iflym.dht.node.ProxyNode? {
        return predecessor
    }

    override fun removeDoPredecessor(oldReference: io.iflym.dht.node.ProxyNode) {
        //处理前缀节点
        if (oldReference == predecessor) {
            this.predecessor = null
        }
    }

    /**
     * 设置指定引用节点为当前节点的前缀(不作其它判断,由调用者判断)
     * 如果之前已有前缀节点,则尝试关闭其代理连接(如果不再需要)
     */
    @Synchronized
    private fun setPredecessor(potentialPredecessor: ProxyNode) {
        Objects.requireNonNull<Any>(potentialPredecessor, "设置前缀节点时相应节点不能为null")

        //如果此值本身就与前缀相同,则直接返回,不需要其它处理
        if (potentialPredecessor == predecessor)
            return

        val previousPredecessor = this.predecessor
        this.predecessor = potentialPredecessor
        if (previousPredecessor != null) {
            //尝试断开代理
            this.disconnectIfUnreferenced(previousPredecessor)
            //因为是新添加的前缀,意味着之后复制的副本可能需要被删除掉
            // 对于后缀节点来说,只有最后一个需要被删除,即最后一个不再需要存储之前前缀节点的副本数据
            //这里需要删除的仅表示源前缀有但新前缀没有的副本数据
            val sLSize = this.successorList.size()
            //仅在满了的情况,没有满的话,就表示本身后缀点数就不够,意味着所有后缀都在源前缀的后缀覆盖范围内
            if (this.successorList.capacity == sLSize) {
                val lastSuccessor = this.successorList.successorList[sLSize - 1]
                try {
                    lastSuccessor.notifyDeleteReplicas(this.predecessor!!.nodeId, emptySet())
                } catch (e: CommunicationException) {
                    logger.warn("在设置新前缀节点时移除多余副本出现通信异常:{}", e.message, e)
                }
            }
            this.logger.debug("源前缀已经被新前缀替换了.源前缀:{},新前缀:{}", previousPredecessor, potentialPredecessor)
        } else {
            //因为之前没有前缀，因此当前节点的数据并没有复制到后缀节点中，这里有了之至则需要进行此操作
            val entriesToRep = this.node.notifyGetEntriesInterval<Any>(predecessor!!.nodeId, nodeId)
            val copyedEntries = mutableMapOf<Id, Entry<Any>>()
            entriesToRep.forEach { copyedEntries[it.key.id] = it }
            val successors = this.successorList.successorList
            for (successor in successors) {
                try {
                    successor.notifyInsertReplicas(copyedEntries)
                } catch (e: CommunicationException) {
                    this.logger.warn("在添加新前缀时复制副本到后缀节点时出现通信异常.节点:{},异常信息:{}", successor.nodeId, e.message, e)
                }
            }
        }
    }

    /**
     * 将一个引用节点添加到引用表中(如果合适)
     * 此引用节点可能是当前节点的前缀节点,但是需要被判断
     *
     * @param potentialPredecessor 潜在(maybe)的前缀节点
     */
    fun addReferenceAsPredecessor(potentialPredecessor: RingNode) {
        Objects.requireNonNull<Any>(potentialPredecessor, "在添加潜在前缀节点时相应节点不能为null")

        val thisProxy = potentialPredecessor as ProxyNode

        //判断替换源前缀节点的场景,即原来前缀为null,或者参数节点更合适作前缀节点(即满足 pre < X < current)
        if (this.predecessor == null || potentialPredecessor.nodeId.isInInterval(this.predecessor!!.nodeId, nodeId)) {
            this.logger.info("发现潜在前缀,准备进行设置或替换.原前缀:{},新前缀:{}",
                    if (this.predecessor == null) "null" else this.predecessor, potentialPredecessor)

            this.setPredecessor(thisProxy)
        }

        //不合适当前缀,则添加到其它地方
        this.addReference(thisProxy)
    }
}