package io.iflym.dht.storage

import io.iflym.dht.model.Entry
import io.iflym.dht.model.Id
import io.iflym.dht.model.Url
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentSkipListMap

/**
 * 在一个本地节点中所有已存储的数据信息,通过一个简单的map进行存储,将当前节点所要处理的数据均存储起来
 * 因为允许多个值允许可能有相同的计算id,因此相同id的存储数据会聚合起来进行存储
 *
 * @author flym
 */
internal class LocalDataStorage : DataStorage {

    /** 实际本地存储数据的并发结构  */
    private val entriesMap = ConcurrentHashMap<String, NavigableMap<Id, Entry<*>>>()

    private fun nodeEntryMap(nodeUrl: Url): NavigableMap<Id, Entry<*>> {
        return entriesMap.computeIfAbsent(nodeUrl.uniqueString()) { ConcurrentSkipListMap() }
    }

    /**
     * 将相应的数据集合进行数据存储
     *
     * @param entriesToAdd 需要存储的数据集合
     */
    override fun <T> addAll(nodeUrl: Url, entriesToAdd: Map<Id, Entry<T>>) {
        entriesToAdd.forEach { this.add(nodeUrl, it.key, it.value) }
    }

    /**
     * 添加一个存储数据
     *
     * @param entryToAdd 需要存储的数据
     */
    override fun <T> add(nodeUrl: Url, id: Id, entryToAdd: Entry<T>) {
        Objects.requireNonNull<Any>(entryToAdd, "待添加数据条目不能为null")

        nodeEntryMap(nodeUrl)[id] = entryToAdd
    }

    /**
     * 移除指定的存储数据
     *
     * @param id 需要移除的数据id
     */
    override fun remove(nodeUrl: Url, id: Id) {
        Objects.requireNonNull<Any>(id, "待移除数据不能为null")

        nodeEntryMap(nodeUrl).remove(id)
    }

    /**
     * 获取指定id下的存储数据,如果没有数据,则返回空集合
     *
     * @param id 相应存储数据的计算id值
     */
    @Suppress("UNCHECKED_CAST")
    override fun <T> getEntry(nodeUrl: Url, id: Id): Entry<T>? {
        Objects.requireNonNull<Any>(id, "获取数据时id值不能为null")

        return nodeEntryMap(nodeUrl)[id] as Entry<T>
    }

    override fun containsEntries(nodeUrl: Url, id: Id): Boolean {
        return nodeEntryMap(nodeUrl).containsKey(id)
    }

    /**
     * 获取所有 min > X <= max的存储数据值, 下界不包括,上界包括
     *
     * @param fromID 存储数据的下界值,不包括在计算中
     * @param toID   存储数据的上界值,包括在计算中
     */
    @Suppress("UNCHECKED_CAST")
    override fun <T> getEntriesInInterval(nodeUrl: Url, fromID: Id, toID: Id): Map<Id, Entry<T>> {
        Objects.requireNonNull<Any>(fromID, "获取数据时下界值不能为null")
        Objects.requireNonNull<Any>(toID, "获取数据时上界值不能为null")

        val entryMap = nodeEntryMap(nodeUrl)

        val result: MutableMap<Id, Entry<*>> = mutableMapOf()

        if (fromID < toID) {
            result.putAll(entryMap.subMap(fromID, false, toID, true))
        } else {
            val firstMap = entryMap.subMap(fromID, false, Id.MAX, true)
            val secondMap = entryMap.subMap(Id.MIN, false, toID, true)
            result.putAll(firstMap)
            result.putAll(secondMap)
        }

        return result as Map<Id, Entry<T>>
    }

    /**
     * 批量移除存储数据
     *
     * @param entriesToRemove 要移除的数据集合
     */
    override fun removeAll(nodeUrl: Url, entriesToRemove: Set<Id>) {
        Objects.requireNonNull(entriesToRemove, "批量移除时条目不能为null")

        entriesToRemove.forEach { this.remove(nodeUrl, it) }
    }

    /** 获取当前存储的所有数据  */
    @Suppress("UNCHECKED_CAST")
    override fun <T> getEntries(nodeUrl: Url): Map<Id, Entry<T>> {
        return Collections.unmodifiableMap(nodeEntryMap(nodeUrl)) as Map<Id, Entry<T>>
    }

    @Suppress("UNCHECKED_CAST")
    override fun <T> getEntries4Debug(nodeUrl: Url): Map<Id, Entry<T>> {
        return Collections.unmodifiableMap(nodeEntryMap(nodeUrl)) as Map<Id, Entry<T>>
    }

    /** 获取当前的存储容量大小  */
    override fun getNumberOfStoredEntries(nodeUrl: Url): Int {
        return nodeEntryMap(nodeUrl).size
    }

    override fun toDebugString(nodeUrl: Url): String {
        val result = StringBuilder("Entries:\n")
        for ((key, value) in nodeEntryMap(nodeUrl)) {
            result.append("  key = ").append(key.toString()).append(", value = ").append(value).append("\n")
        }
        return result.toString()
    }

    companion object {
        val instance = LocalDataStorage()
    }
}