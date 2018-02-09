package io.iflym.dht.storage

import io.iflym.dht.model.Entry
import io.iflym.dht.model.Id
import io.iflym.dht.model.Url

/**
 * 描述统一的下层实际存储
 * Created by tanhua(flym) on 7/17/2017.
 * @author flym
 **/
interface DataStorage {
    /** 批量添加多个数据  */
    fun <T> addAll(nodeUrl: Url, entriesToAdd: Map<Id, Entry<T>>)

    /**
     * 添加一个存储数据
     *
     * @param nodeUrl    哪个节点添加
     * @param entryToAdd 需要存储的数据
     */
    fun <T> add(nodeUrl: Url, id: Id, entryToAdd: Entry<T>)

    /**
     * 移除指定的存储数据
     *
     * @param nodeUrl       哪个节点删除
     * @param id 需要移除的数据id
     */
    fun remove(nodeUrl: Url, id: Id)

    /**
     * 获取指定id下的存储数据,如果没有数据,则返回null
     *
     * @param id 相应存储数据的计算id值
     */
    fun <T> getEntry(nodeUrl: Url, id: Id): Entry<T>?

    /** 判断是否存在指定id的存储数据  */
    fun containsEntries(nodeUrl: Url, id: Id): Boolean

    /**
     * 获取所有 min > X <= max的存储数据值, 下界不包括,上界包括
     *
     * @param fromID 存储数据的下界值,不包括在计算中
     * @param toID   存储数据的上界值,包括在计算中
     */
    fun <T> getEntriesInInterval(nodeUrl: Url, fromID: Id, toID: Id): Map<Id, Entry<T>>

    /**
     * 批量移除存储数据
     *
     * @param entriesToRemove 要移除的数据集合
     */
    fun removeAll(nodeUrl: Url, entriesToRemove: Set<Id>)

    /** 获取当前存储的所有数据,如果不能返回数据,则返回空集合  */
    fun <T> getEntries(nodeUrl: Url): Map<Id, Entry<T>>

    /** 获取当前的存储容量大小,如果不能返回,则返回-1  */
    fun getNumberOfStoredEntries(nodeUrl: Url): Int

    /** 获取当前存储的所有数据(这里忽略性能因素)  */
    fun <T> getEntries4Debug(nodeUrl: Url): Map<Id, Entry<T>>

    /** 返回字符串描述(用于debug,忽略性能)  */
    fun toDebugString(nodeUrl: Url): String
}
