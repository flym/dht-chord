package io.iflym.dht.reference

import io.iflym.dht.exception.CommunicationException
import io.iflym.dht.model.Id
import io.iflym.dht.node.ProxyNode
import io.iflym.dht.node.RingNode
import io.iflym.dht.service.chord.Chord
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.*

/** Created by flym on 7/26/2017.  */
abstract class AbstractReferences protected constructor(_node: RingNode) : References {

    protected val logger: Logger = LoggerFactory.getLogger(References::class.java.name + "." + _node.nodeId)

    override val nodeId: Id = _node.nodeId

    protected val node = _node

    protected fun consumeWithFingerTable(consumer: (FingerTable) -> Unit) {
        Optional.ofNullable(fingerTable()).ifPresent(consumer)
    }

    protected fun consumeWithSuccessorList(consumer: (SuccessorList) -> Unit) {
        Optional.ofNullable(successorList()).ifPresent(consumer)
    }

    protected fun consumeWithPredecessor(consumer: (ProxyNode) -> Unit) {
        Optional.ofNullable(predecessor()).ifPresent(consumer)
    }

    protected fun <T> handleWithFingerTable(function: (FingerTable) -> T): T {
        val fingerTable = fingerTable()
        return function(fingerTable)
    }

    protected fun <T> handleWithSuccessorList(function: (SuccessorList) -> T): T {
        val fingerTable = successorList()
        return function(fingerTable)
    }

    protected fun <T> handleWithPredecessor(function: (ProxyNode) -> T, elseSupplier: () -> T): T {
        val predecessor = predecessor()
        return if (predecessor != null) function(predecessor) else elseSupplier()

    }

    /** 没有找到最接近id值的前缀节点  */
    protected open fun missedGetValidClosestPrecedingNode(key: Id): RingNode? {
        return null
    }

    /** 尝试从多方信息(前缀,后缀,索引表)中找到指定id最接近其值的前缀节点(作节点存活性判断)  */
    @Synchronized
    override fun getValidClosestPrecedingNode(key: Id): RingNode? {
        Objects.requireNonNull<Any>(key, "在查找最近前缀节点时id值不能为null")

        //准备从索引表,后缀表,以及前缀中都进行查找,最后再综合结果找一个最接近的

        val foundNodes = HashMap<Id, RingNode>()

        //从索引表中查找
        consumeWithFingerTable({ t ->
            val closestNodeFT = t.getValidClosestPrecedingNode(key)
            if (closestNodeFT != null) {
                foundNodes[closestNodeFT.nodeId] = closestNodeFT
            }
        })

        //从后缀表中查找
        consumeWithSuccessorList({ t ->
            val closestNodeSL = t.getValidClosestPrecedingNode(key)
            if (closestNodeSL != null) {
                foundNodes[closestNodeSL.nodeId] = closestNodeSL
            }
        })

        //从前缀中找,即前缀也可能是一个可选项
        consumeWithPredecessor({ t ->
            if (t.isCurrentAlive && key.isInInterval(t.nodeId, nodeId)) {
                foundNodes[t.nodeId] = t
            }
        })


        if (foundNodes.isEmpty())
            return missedGetValidClosestPrecedingNode(key)

        //基于以下的查找算法,将刚才的所有key放入列表中,再加上参数key,排序,然后在key之前的那一个即是找到的值

        val orderedIDList = ArrayList(foundNodes.keys)
        orderedIDList.add(key)
        val sizeOfList = orderedIDList.size

        //排序,再定位到参数key,前一位即是找到的值
        //排序
        Collections.sort(orderedIDList)
        val keyIndex = orderedIDList.indexOf(key)
        //如果key在第1位,则列表中最后一个即是找到值
        val index = (sizeOfList + (keyIndex - 1)) % sizeOfList

        val idOfclosestNode = orderedIDList[index]
        return foundNodes[idOfclosestNode]
    }

    /**
     * 添加新的引用节点在索引表以及后缀表中(如果合适)
     * 此节点应该不是当前节点的前缀
     * 如果是前缀节点,则应该调用[io.iflym.dht.node.persistent.PersistentReferences.addReferenceAsPredecessor]方法
     *
     * @param newReference 要添加的引用节点
     */
    @Synchronized
    override fun addReference(newReference: ProxyNode) {
        //不允许添加自己
        if (newReference.nodeId == nodeId) {
            logger.debug("不能够添加自己到引用表中")
            return
        }

        consumeWithFingerTable({ t -> t.addReference(newReference) })
        consumeWithSuccessorList({ t -> t.addSuccessor(newReference) })

        newReference.addProxyListener(this)
    }

    override fun findSuccessorWithinReferences(id: Id): RingNode? {
        //从引用表中查找最接近于查找节点(刚好比key小的节点),再跳转至其进行查找
        val closestPrecedingNode = getValidClosestPrecedingNode(id) ?: return null

        return try {
            this.logger.debug("将请求分派给最近的节点进行处理.最近节点值:{},查找值:{}", closestPrecedingNode.nodeId, id)
            closestPrecedingNode.notifyFindValidNode(id)
        } catch (e: CommunicationException) {
            this.logger.error("分派请求时出现网络通信错误,将移除此节点.查找值:{},分派节点:{},错误信息:{}", id, closestPrecedingNode, e.message, e)
            removeReference(closestPrecedingNode as ProxyNode)
            findSuccessorWithinReferences(id)
        }

    }

    override fun findSuccessorWithinReferences4Get(id: Id): RingNode? {
        val closestPrecedingNode = getValidClosestPrecedingNode(id) ?: return null
        return try {
            logger.debug("为查询目的将请求分派给最近的节点进行处理.最近节点值:{},查找值:{}", closestPrecedingNode.nodeId, id)
            closestPrecedingNode.notifyFindValidNode4Get(id)
        } catch (e: CommunicationException) {
            logger.error("为查询目的分派请求时出现网络通信错误,将移除此节点.查找值:{},分派节点:{},错误信息:{}", id, closestPrecedingNode, e.message, e)
            removeReference(closestPrecedingNode as ProxyNode)
            findSuccessorWithinReferences4Get(id)
        }
    }

    protected abstract fun removeDoPredecessor(oldReference: ProxyNode)

    /**
     * 从索引表及后缀表中移除相应的引用节点
     * 如果移除的节点是当前节点的前缀,则将当前前缀置null
     */
    @Synchronized
    override fun removeReference(oldReference: ProxyNode) {
        Objects.requireNonNull<Any>(oldReference, "移除引用节点时相应节点不能为null")

        consumeWithFingerTable({ t -> t.removeReference(oldReference) })
        consumeWithSuccessorList({ t -> t.removeReference(oldReference) })
        consumeWithPredecessor({ _ -> removeDoPredecessor(oldReference) })

        //处理可能在代理回收
        disconnectIfUnreferenced(oldReference)
    }

    @Synchronized
    override fun toString(): String {
        val result = StringBuilder("Node: " + nodeId + "\n")
        consumeWithFingerTable { result.append(it) }
        consumeWithSuccessorList { result.append(it) }
        consumeWithPredecessor({ t -> result.append("Predecessor:").append(t.nodeId).append(",").append(t.nodeUrl) })

        return result.toString()
    }

    /**
     * 判断指定的网络节点是否在当前引用表中(包括前缀,后缀,索引表),以用于某些场景中需要清除此代理节点的情况
     *
     * @param newReference 要查询的引用节点
     */
    override fun containsReference(newReference: ProxyNode): Boolean {
        Objects.requireNonNull<Any>(newReference, "在进行引用节点contain判断时节点不能为null")

        val elseSupplier = { java.lang.Boolean.FALSE }

        return (handleWithFingerTable({ t -> t.containsReference(newReference) })
                || handleWithSuccessorList({ t -> t.containsReference(newReference) })
                || handleWithPredecessor({ t -> newReference == t }, elseSupplier))
    }

    override fun clearAndDisconnect() {
        //前缀
        consumeWithPredecessor { it.disconnect() }

        //索引表
        consumeWithFingerTable({ t ->
            t.copyOfReferences.asSequence().filter { it != null }.forEach { it!!.disconnect() }
            t.clear()
        })

        //后缀表
        consumeWithSuccessorList({ t ->
            t.successorList.forEach { it.disconnect() }
            t.clear()
        })
    }


    companion object {
        /** 每个节点在引用表中需要维护的后缀节点的数目,最小为1  */
        val NUMBER_OF_SUCCESSORS: Int

        init {
            val numberOfSuccessor = Integer.parseInt(System.getProperty(Chord.PROPERTY_PREFIX + ".successors"))
            NUMBER_OF_SUCCESSORS = if (numberOfSuccessor < 1) 1 else numberOfSuccessor
        }
    }

}
