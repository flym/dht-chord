package io.iflym.dht.storage

import com.google.common.collect.ImmutableMap
import com.google.common.collect.Lists
import com.google.common.collect.Maps
import com.iflym.core.util.ExceptionUtils
import io.iflym.dht.model.Entry
import io.iflym.dht.model.Id
import io.iflym.dht.model.Url
import io.iflym.dht.util.AutoCloseableUtils
import io.iflym.dht.util.ByteUtils
import io.iflym.dht.util.JsonUtils
import org.rocksdb.*
import org.rocksdb.RocksDB.DEFAULT_COLUMN_FAMILY
import org.slf4j.LoggerFactory
import java.io.Closeable
import java.util.*

/**
 * 使用rocksdb进行的数据存储
 * Created by tanhua(flym) on 7/17/2017.
 *
 * @author flym
 */
class RocksDataStorage private constructor() : DataStorage, Closeable {


    private lateinit var rocksDB: RocksDB
    private val familyNameList = Lists.newArrayList<ByteArray>()
    private lateinit var familyOptions: ColumnFamilyOptions
    private val familyHandleList = Lists.newArrayList<ColumnFamilyHandle>()
    private val familyDescriptorList = Lists.newArrayList<ColumnFamilyDescriptor>()
    private val closeableList = Lists.newArrayList<AutoCloseable>()

    @Throws(RocksDBException::class)
    private fun init(path: String) {
        var exception = false
        try {
            val options = DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true)
            closeableList.add(options)

            familyOptions = ColumnFamilyOptions().optimizeUniversalStyleCompaction()
            closeableList.add(familyOptions)
            //使用特别的顺序,即按原始bit进行排序
            familyOptions.setComparator(object : DirectComparator(ComparatorOptions()) {
                override fun name(): String {
                    return "chordComparator"
                }

                override fun compare(a: DirectSlice, b: DirectSlice): Int {
                    return ByteUtils.compareBitTo(a.data(), b.data())
                }
            })

            val oldFamilyNameList = RocksDB.listColumnFamilies(Options().setCreateIfMissing(true), path)

            //添加默认的列簇
            if (!contains(oldFamilyNameList, DEFAULT_COLUMN_FAMILY)) {
                familyNameList.add(DEFAULT_COLUMN_FAMILY)
                familyDescriptorList.add(ColumnFamilyDescriptor(DEFAULT_COLUMN_FAMILY, familyOptions))
            }

            familyNameList.addAll(oldFamilyNameList)
            oldFamilyNameList.forEach { t -> familyDescriptorList.add(ColumnFamilyDescriptor(t, familyOptions)) }

            rocksDB = RocksDB.open(options, path, familyDescriptorList, familyHandleList)
            closeableList.add(rocksDB)
        } catch (e: Exception) {
            log.error("初始化rocksdb时有异常发生:{}", e.message, e)
            exception = true
            throw RuntimeException(e.message, e)
        } finally {
            if (exception) {
                close()
            }
        }
    }

    override fun close() {
        closeableList.forEach { AutoCloseableUtils.closeQuietly(it) }
    }

    fun addNodeUrl(nodeUrl: Url) {
        val bytes = nodeUrl.uniqueString().toByteArray()
        if (!contains(familyNameList, bytes)) {
            val familyDescriptor = ColumnFamilyDescriptor(bytes, familyOptions)
            val familyHandle: ColumnFamilyHandle
            try {
                familyHandle = rocksDB.createColumnFamily(familyDescriptor)

                familyHandleList.add(familyHandle)
                familyDescriptorList.add(familyDescriptor)
                familyNameList.add(bytes)
            } catch (e: RocksDBException) {
                throw RuntimeException(e.message, e)
            }

        }
    }

    private fun findHandle(nodeUrl: Url): ColumnFamilyHandle {
        return Optional.ofNullable(findHandle(familyNameList, familyHandleList, nodeUrl.uniqueString().toByteArray()))
                .orElseThrow { IllegalArgumentException("没有此路径存储处理器,请通过addNodeUrl添加." + nodeUrl) }
    }

    override fun <T> addAll(nodeUrl: Url, entriesToAdd: Map<Id, Entry<T>>) {
        val handle = findHandle(nodeUrl)
        WriteOptions().let { options ->
            WriteBatch().let { writeBatch ->
                entriesToAdd.forEach { t -> writeBatch.put(handle, t.key.id, entryToBytes(t.value)) }
                ExceptionUtils.doActionRethrowE { rocksDB.write(options, writeBatch) }
            }
        }
    }

    override fun <T> add(nodeUrl: Url, id: Id, entryToAdd: Entry<T>) {
        val handle = findHandle(nodeUrl)
        val bytes = entryToBytes(entryToAdd)
        ExceptionUtils.doActionRethrowE { rocksDB.put(handle, id.id, bytes) }
    }

    override fun remove(nodeUrl: Url, id: Id) {
        val handle = findHandle(nodeUrl)
        ExceptionUtils.doActionRethrowE { rocksDB.delete(handle, id.id) }
    }

    override fun <T> getEntry(nodeUrl: Url, id: Id): Entry<T>? {
        val handle = findHandle(nodeUrl)
        return ExceptionUtils.doFunRethrowE {
            val bytes = rocksDB.get(handle, id.id) ?: return@doFunRethrowE null

            return@doFunRethrowE bytesToEntry<T>(bytes)
        }
    }

    override fun containsEntries(nodeUrl: Url, id: Id): Boolean {
        val handle = findHandle(nodeUrl)
        return ExceptionUtils.doFunRethrowE {
            val builder = StringBuilder()
            val result = rocksDB.keyMayExist(handle, id.id, builder)
            return@doFunRethrowE result && builder.isNotEmpty()
        }
    }

    private fun <T> iterate(nodeUrl: Url, from: ByteArray, to: ByteArray): Map<Id, Entry<T>> {
        val handle = findHandle(nodeUrl)
        return ExceptionUtils.doFunRethrowE<Map<Id, Entry<T>>> {
            val set = mutableMapOf<Id, Entry<T>>()
            rocksDB.newIterator(handle).let { iterator ->
                iterator.seek(from)
                while (iterator.isValid) {
                    val key: ByteArray = iterator.key() as ByteArray
                    if (ByteUtils.compareBitTo(key, to) < 0)
                        break

                    val entry = bytesToEntry<T>(iterator.value())
                    set[Id(key)] = entry

                    iterator.next()
                }
                iterator.close()
            }
            set
        }
    }

    override fun <T> getEntriesInInterval(nodeUrl: Url, fromID: Id, toID: Id): Map<Id, Entry<T>> {
        val fromKey = fromID.id
        val toKey = toID.id
        //暂处理from < to的情况
        if (fromID < toID) {
            return iterate(nodeUrl, fromKey, toKey)
        }

        val resultSet = mutableMapOf<Id, Entry<T>>()
        resultSet.putAll(iterate(nodeUrl, fromKey, Id.MAX.id))
        resultSet.putAll(iterate(nodeUrl, Id.MIN.id, toKey))

        return resultSet
    }

    override fun removeAll(nodeUrl: Url, entriesToRemove: Set<Id>) {
        val handle = findHandle(nodeUrl)
        WriteOptions().let { options ->
            WriteBatch().let { writeBatch ->
                entriesToRemove.forEach { writeBatch.remove(handle, it.id) }
                ExceptionUtils.doActionRethrowE { rocksDB.write(options, writeBatch) }
            }
        }
    }

    override fun <T> getEntries(nodeUrl: Url): Map<Id, Entry<T>> {
        return ImmutableMap.of()
    }

    override fun getNumberOfStoredEntries(nodeUrl: Url): Int {
        return -1
    }

    override fun <T> getEntries4Debug(nodeUrl: Url): Map<Id, Entry<T>> {
        val handle = findHandle(nodeUrl)
        val resultMap = Maps.newHashMap<Id, Entry<T>>()
        rocksDB.newIterator(handle).let { iterator ->
            iterator.seekToFirst()
            while (iterator.isValid) {
                val key = iterator.key()
                val entry = JsonUtils.parse<Entry<T>>(iterator.value())
                val keyId = Id(key)
                resultMap[keyId] = entry

                iterator.next()
            }
        }

        return resultMap
    }

    override fun toDebugString(nodeUrl: Url): String {
        val result = StringBuilder("RocksDbEntries:\n")
        for ((key, value) in getEntries4Debug<Any>(nodeUrl)) {
            result.append("  key = ").append(key.toString()).append(", value = ").append(value).append("\n")
        }
        return result.toString()
    }

    companion object {
        private val log = LoggerFactory.getLogger(RocksDataStorage::class.java)
        val instance by lazy { buildInstance() }

        val replicaInstance by lazy { buildReplicaInstance() }

        private fun buildInstance(): RocksDataStorage {
            val value = RocksDataStorage()
            val path = System.getProperty("chord.rocksdb.path")
            ExceptionUtils.doActionRethrowE { value.init(path) }
            return value
        }

        private fun buildReplicaInstance(): RocksDataStorage {
            val value = RocksDataStorage()
            val path = System.getProperty("chord.rocksdb.replica.path")
            ExceptionUtils.doActionRethrowE { value.init(path) }
            return value
        }

        private fun contains(byteList: List<ByteArray>, bytes: ByteArray): Boolean {
            return byteList.stream().anyMatch { t -> Arrays.equals(t, bytes) }
        }

        private fun findHandle(byteList: List<ByteArray>, handleList: List<ColumnFamilyHandle>, bytes: ByteArray): ColumnFamilyHandle? {
            return byteList.indices
                    .firstOrNull { Arrays.equals(byteList[it], bytes) }
                    ?.let { handleList[it] }
        }

        private fun <T> bytesToEntry(bytes: ByteArray): Entry<T> {
            return JsonUtils.parse(bytes)
        }

        private fun <T> entryToBytes(e: Entry<T>): ByteArray {
            return JsonUtils.toJson(e)
        }
    }
}
