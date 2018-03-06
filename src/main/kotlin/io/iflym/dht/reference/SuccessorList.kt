package io.iflym.dht.reference

import io.iflym.dht.model.Id
import io.iflym.dht.node.ProxyNode

/**
 * 描述一个后缀表接口应该做的事
 * Created by flym on 7/26/2017.
 */
interface SuccessorList {

    /** 返回不可变的后缀表数  */
    val successorList: List<ProxyNode>

    /** 拿到后缀表中第1个后缀节点(如果有),否则 返回null  */
    val directSuccessor: ProxyNode?

    /** 添加一个新的后缀节点,并且添加之后后缀表仍然是有序的  */
    fun addSuccessor(nodeToAdd: ProxyNode)

    /**
     * 移除相应的引用,并且填充因为移除之后剩下的位置,即从引用表中查找可以填充的引用数据
     *
     * @param nodeToDelete 需要被移除的后缀节点
     */
    fun removeReference(nodeToDelete: ProxyNode)

    /**
     * 找到对于指定id来说,最接近其的前缀节点,并且当前是有效的
     * 即对于X, current < Y < X,从当前后缀表中倒数开始,第1个满足此条件的即为找到值.
     *
     * @param idToLookup 用于查找并比较的id值
     */
    fun getValidClosestPrecedingNode(idToLookup: Id): ProxyNode?

    /** 判定相应的节点是否在当前后缀列表中  */
    fun containsReference(nodeToLookup: ProxyNode): Boolean

    /** 当前后缀表存储长度  */
    fun size(): Int

    /** 清除后缀表数据  */
    fun clear()
}
