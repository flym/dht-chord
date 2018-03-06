package io.iflym.dht.reference

import io.iflym.dht.model.Id
import io.iflym.dht.node.ProxyNode
import io.iflym.dht.node.RingNode
import io.iflym.dht.node.persistent.PersistentSuccessorList
import io.iflym.dht.util.Asserts
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.*

/** Created by flym on 7/26/2017.  */
open class AbstractSuccessorList protected constructor(
        protected val node: RingNode,
        numberOfEntries: Int,
        /** 反向引用当前节点的引用表信息  */
        protected var references: References
) : SuccessorList {
    /** 当前节点id值  */
    protected var localID: Id = node.nodeId

    /** 用于存储后缀节点的列表  */
    protected lateinit var _successorList: MutableList<ProxyNode>

    /** 返回不可变的后缀表数  */
    override val successorList: List<ProxyNode> by lazy { Collections.unmodifiableList(this._successorList) }

    /** 当前后缀列表的容量值  */
    var capacity: Int = 0
        protected set

    /** 当前后缀表日志对象  */
    protected var logger: Logger

    /** 拿到后缀表中第1个后缀节点(如果有),否则 返回null  */
    override val directSuccessor: ProxyNode?
        get() = if (_successorList.isEmpty()) null else _successorList[0]

    init {
        Objects.requireNonNull<Any>(localID, "构建后缀表时id值不能为null")
        Objects.requireNonNull<Any>(references, "构建后缀表时引用表不能为null")
        Asserts.assertTrue(numberOfEntries >= 1, "后缀表的后缀节点数至少为1")

        this.logger = LoggerFactory.getLogger(PersistentSuccessorList::class.java.toString() + "." + localID)
        this.capacity = numberOfEntries
        this._successorList = LinkedList()
    }

    protected open fun mayCopyReplicas(successorProxy: ProxyNode) {}

    protected open fun mayRemoveReplicas(successorProxy: ProxyNode) {

    }

    /** 添加一个新的后缀节点,并且添加之后后缀表仍然是有序的  */
    override fun addSuccessor(nodeToAdd: ProxyNode) {
        Objects.requireNonNull<Any>(nodeToAdd, "待添加的后缀节点为null")

        //已存在,则直接跳过
        if (this._successorList.contains(nodeToAdd))
            return

        //如果容量已满,并且新的后缀比原来的最末的节点还要大,即理应放到末尾,但容量已满,则不再添加
        if (this._successorList.size >= this.capacity) {
            val oldMaxOld = this._successorList[this._successorList.size - 1]
            if (nodeToAdd.nodeId.isInInterval(oldMaxOld.nodeId, localID)) {
                logger.debug("后缀表已满,待添加的新后缀比原来所有后缀均大,不能添加到后缀表中.原末尾节点:{},待添加后缀值:{}", oldMaxOld, nodeToAdd)
                return
            }
        }

        var inserted = false
        var i = 0
        while (i < this._successorList.size && !inserted) {
            if (nodeToAdd.nodeId.isInInterval(this.localID, this._successorList[i].nodeId)) {
                this._successorList.add(i, nodeToAdd)
                inserted = true
            }
            i++
        }
        //还没有插入到列表中,则放到末尾
        if (!inserted)
            this._successorList.add(nodeToAdd)

        mayCopyReplicas(nodeToAdd)

        //如果超出容量,则移除末尾的数据
        if (this._successorList.size > this.capacity) {
            val nodeToDelete = this._successorList[this._successorList.size - 1]
            this._successorList.remove(nodeToDelete)

            mayRemoveReplicas(nodeToDelete)

            //因为当前节点应该不再需要访问此后缀节点,因此关闭相应的网络代理
            this.references.disconnectIfUnreferenced(nodeToDelete)
        }
    }

    /**
     * 移除相应的引用,并且填充因为移除之后剩下的位置,即从引用表中查找可以填充的引用数据
     *
     * @param nodeToDelete 需要被移除的后缀节点
     */
    override fun removeReference(nodeToDelete: ProxyNode) {
        Objects.requireNonNull<Any>(nodeToDelete, "待移除的后缀节点不能为null")
        this._successorList.remove(nodeToDelete)

        //从索引表中找到用来填坑的引用节点,并添加到当前后缀表中
        //这里找到容量个数个填坑节点,因为大部分都已经添加到后续表中,因此此逻辑最多会添加1个新的坑(或者在过程中又会移除新的后缀),以保证后缀更准备
        val fingerTable = references.fingerTable()

        val referencesOfFingerTable = fingerTable.getFirstFingerTableEntries(this.capacity)
        //这里返回的数据中应该不会包括待移除数据,这里进一步确认(可考虑作断言处理)
        referencesOfFingerTable.filter { it != nodeToDelete }.forEach { addSuccessor(it) }
    }


    override fun toString(): String {
        val result = StringBuilder("Successor List:\n")
        for (next in this._successorList) {
            result.append("  ").append(next.nodeId.toString()).append(", ").append(next.nodeUrl).append("\n")
        }
        return result.toString()
    }

    /**
     * 找到对于指定id来说,最接近其的前缀节点,并且当前是有效的
     * 即对于X, current < Y < X,从当前后缀表中倒数开始,第1个满足此条件的即为找到值.
     *
     * @param idToLookup 用于查找并比较的id值
     */
    override fun getValidClosestPrecedingNode(idToLookup: Id): ProxyNode? {
        Objects.requireNonNull<Any>(idToLookup, "在查找最近前缀节点时的参考id值不能为null")

        //因为是最接近的前缀,因此倒数查找

        return this._successorList.indices.reversed()
                .map { this._successorList[it] }
                .firstOrNull { it.isCurrentAlive && it.nodeId.isInInterval(this.localID, idToLookup) }
    }

    /** 判定相应的节点是否在当前后缀列表中  */
    override fun containsReference(nodeToLookup: ProxyNode): Boolean {
        Objects.requireNonNull<Any>(nodeToLookup, "判断节点是否存在时相应的判断节点不能为null")

        return this._successorList.contains(nodeToLookup)
    }

    /** 当前后缀表存储长度  */
    override fun size(): Int {
        return this._successorList.size
    }

    override fun clear() {
        _successorList.clear()
    }
}
