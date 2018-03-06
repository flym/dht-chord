package io.iflym.dht.reference

import io.iflym.dht.model.Id
import io.iflym.dht.node.ProxyNode

/**
 * 描述一个主要的索引表语义
 * Created by flym on 7/26/2017.
 */
interface FingerTable {

    /** 返回一份索引表copy版本  */
    val copyOfReferences: Array<ProxyNode?>

    /** 1个新的索引值节点进来了,这里检查是否可以加入到索引表中,如果可以则加入到表中,如果原值有值,则覆盖原下标  */
    fun addReference(proxy: ProxyNode)

    /**
     * 从索引表中移除相应的索引值节点,并补上相应的坑
     *
     * @param node 待移除的节点
     */
    fun removeReference(node: ProxyNode)

    /**
     * 找到最接近指定id值的节点的父节点,如果没有则返回null
     * 返回的代理对象是当前可用的,如果是不可用的,则跳过
     *
     * @param key 用于查找的id值
     */
    fun getValidClosestPrecedingNode(key: Id): ProxyNode?

    /** 判断指定节点是否在索引表中  */
    fun containsReference(newReference: ProxyNode): Boolean

    /** 查找索引表中前X个非重复的索引节点,如果少于要求数,则返回已有的  */
    fun getFirstFingerTableEntries(i: Int): List<ProxyNode>

    /** 清除所有代理  */
    fun clear()

}
