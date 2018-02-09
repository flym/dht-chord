package io.iflym.dht.reference

import io.iflym.dht.model.Id
import io.iflym.dht.model.Url
import io.iflym.dht.node.ProxyNode
import io.iflym.dht.util.Asserts
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.*

/**
 * 维护当前节点的索引表,用于快速定位数据查找和处理
 *
 * @author flym
 */
abstract class AbstractFingerTable
(
        /** 当前的id值信息  */
        protected var localID: Id,
        /** 反向引用引用表  */
        protected var references: References
) : FingerTable {

    /** 引用表中基于指定id值的快速访问节点列表(以2的N次方为范围值,共id值位长度)  */
    private var remoteNodes: Array<ProxyNode?>

    /** 当前索引表的日志对象  */
    protected var logger: Logger

    /** 返回一份索引表copy版本  */
    override val copyOfReferences: Array<ProxyNode?>
        get() = Arrays.copyOf(remoteNodes, remoteNodes.size)

    init {
        Objects.requireNonNull<Any>(localID, "构建索引表时id值不能为null")
        Objects.requireNonNull<Any>(references, "构建索引表时引用表不能为null")

        this.logger = LoggerFactory.getLogger(AbstractFingerTable::class.java.toString() + "." + localID)
        this.remoteNodes = arrayOfNulls<ProxyNode?>(localID.length)
    }

    private fun assertIndexInRange(index: Int) {
        Asserts.assertTrue(index >= 0 && index < remoteNodes.size, ArrayIndexOutOfBoundsException::class.java, "索引值:{}超出范围", index)
    }

    /** 为指定的下标位设置索引值节点  */
    private fun setEntry(index: Int, proxy: ProxyNode) {
        assertIndexInRange(index)
        Objects.requireNonNull<Any>(proxy, "索引值节点不能为null")

        this.remoteNodes[index] = proxy
    }

    /** 获取指定下标位的索引值节  */
    private fun getEntry(index: Int): ProxyNode? {
        assertIndexInRange(index)

        return this.remoteNodes[index]
    }

    /** 将指定位原索引值节点置null,并尝试断掉相应的链接  */
    private fun unsetEntry(index: Int) {
        assertIndexInRange(index)

        val overwrittenNode = this.getEntry(index)

        this.remoteNodes[index] = null

        if (overwrittenNode != null) {
            this.references.disconnectIfUnreferenced(overwrittenNode)
            this.logger.debug("指定下标位已经置null,并断开代理连接.位:{},原索引值节点:{}", index, overwrittenNode)
        }
    }

    /** 1个新的索引值节点进来了,这里检查是否可以加入到索引表中,如果可以则加入到表中,如果原值有值,则覆盖原下标  */
    override fun addReference(proxy: ProxyNode) {
        Objects.requireNonNull<Any>(proxy, "添加索引值节点时相应节点不能为null")

        var lowestWrittenIndex = -1
        var highestWrittenIndex = -1

        for (i in this.remoteNodes.indices) {

            val startOfInterval = this.localID.addPowerOfTwo(i)
            //此下标计算的id值已超过proxy节点处理的范围了,因此跳过
            if (!startOfInterval.isInInterval(this.localID, proxy.nodeId)) {
                break
            }

            val oldEntry = getEntry(i)
            //原地方为null,可以替换
            val oldEntryNull = oldEntry == null
            //原地方比当前值更大,表示当前值更适合放在此位置
            val oldEntryNeedReplace = !oldEntryNull && proxy.nodeId.isInInterval(this.localID, oldEntry!!.nodeId)
            if (oldEntryNull || oldEntryNeedReplace) {
                if (lowestWrittenIndex == -1) {
                    lowestWrittenIndex = i
                }
                highestWrittenIndex = i

                setEntry(i, proxy)

                //原被替换了,原值代理就可能不再需要了
                if (oldEntryNeedReplace) {
                    this.references.disconnectIfUnreferenced(oldEntry!!)
                }
            }
        }

        if (highestWrittenIndex == -1) {
            logger.debug("待添加的索引值节点并没有被添加到索引表中")
        } else {
            if (highestWrittenIndex == lowestWrittenIndex) {
                logger.info("添加索引值节点到下标:{},索引节点:{}", highestWrittenIndex, proxy)
            } else {
                logger.info("添加索引值节点为一个范围,从:{}到:{},索引节点:{}", lowestWrittenIndex, highestWrittenIndex, proxy)
            }
        }
    }

    override fun toString(): String {
        val result = StringBuilder("Finger table:\n")

        var lastIndex = -1
        var lastNodeID: Id? = null
        var lastNodeURL: Url? = null
        for (i in this.remoteNodes.indices) {
            val next = this.remoteNodes[i]
            if (next == null) {
                //当前为null,而上一步不为null,因此记录从之前的下标到当前上一步
                if (lastIndex != -1 && lastNodeID != null) {
                    val preIndex = i - 1
                    result.append("  ").append(lastNodeID).append(", ").append(lastNodeURL).append(" ")
                            .append(if (preIndex - lastIndex > 0) "($lastIndex-$preIndex)" else "($preIndex)")
                            .append("\n")

                    //因为当前值为null,重置相应记数,到下一个不为null为止
                    lastIndex = -1
                    lastNodeID = null
                    lastNodeURL = null
                }
            } else if (lastNodeID == null) {
                //初始记数(自上一次为null而来)
                lastIndex = i
                lastNodeID = next.nodeId
                lastNodeURL = next.nodeUrl
            } else if (lastNodeID != next.nodeId) {
                //当前值与上一次不一样,因为需要记录上一次的范围值了
                val preIndex = i - 1
                result.append("  ").append(lastNodeID).append(", ").append(lastNodeURL).append(" ")
                        .append(if (preIndex - lastIndex > 0) "($lastIndex-$preIndex)" else "($preIndex)")
                        .append("\n")
                //上一次已记录,最新值从当前节点开始
                lastNodeID = next.nodeId
                lastNodeURL = next.nodeUrl
                lastIndex = i
            }
        }

        //追加最后一次记录
        if (lastNodeID != null) {
            val last = this.remoteNodes.size - 1
            result.append("  ").append(lastNodeID).append(", ").append(lastNodeURL).append(" ")
                    .append(if (last - lastIndex > 0) "($lastIndex-$last)" else "($last)")
                    .append("\n")
        }

        return result.toString()
    }

    /**
     * 从索引表中移除相应的索引值节点,并补上相应的坑
     *
     * @param node 待移除的节点
     */
    override fun removeReference(node: ProxyNode) {
        Objects.requireNonNull<Any>(node, "移除索引值节点时此节点不能为null")

        // for logging
        var lowestWrittenIndex = -1
        var highestWrittenIndex = -1

        //因为移除了相应的节点,这里找到用来替代移除节点的值,即填坑节点
        //这里要倒数找,因为填坑的是原来的后缀值
        val referenceForReplacement: ProxyNode? = (this.localID.length - 1 downTo 0)
                .asSequence()
                .mapNotNull { this.getEntry(it) }
                .takeWhile { node != it }
                .lastOrNull()

        //移除相应的索引值节点,并填上相应的坑位
        for (i in this.remoteNodes.indices) {
            if (node == this.remoteNodes[i]) {
                if (lowestWrittenIndex == -1) {
                    lowestWrittenIndex = i
                }
                highestWrittenIndex = i

                if (referenceForReplacement == null) {
                    //没填坑的,则直接置空
                    unsetEntry(i)
                } else {
                    setEntry(i, referenceForReplacement)
                }
            }
        }

        postRemoveReference(node)

        when (highestWrittenIndex) {
            -1 -> this.logger.debug("待移除的索引值节点不需要在索引表中处理,因为它并不在索引表中")
            lowestWrittenIndex -> this.logger.debug("从索引表中移除节点,原下标位:{},索引值:{}", highestWrittenIndex, node)
            else -> this.logger.debug("从索引表中移除节点,从:{}到:{},索引值:{}", lowestWrittenIndex, highestWrittenIndex, node)
        }
    }

    protected open fun postRemoveReference(node: ProxyNode) {
        //nothing to do
    }

    /**
     * 找到最接近指定id值的节点的父节点,如果没有则返回null
     * 返回的代理对象是当前可用的,如果是不可用的,则跳过
     *
     * @param key 用于查找的id值
     */
    override fun getValidClosestPrecedingNode(key: Id): ProxyNode? {
        Objects.requireNonNull<Any>(key, "查找最近前缀节点时指定id值不能为null")
        //因为是最接近查找,因此倒序查找,

        return this.remoteNodes.indices.reversed()
                .asSequence()
                .map { this.remoteNodes[it] }
                .firstOrNull { it != null && it.isCurrentAlive && it.nodeId.isInInterval(this.localID, key) }
    }

    /** 判断指定节点是否在索引表中  */
    override fun containsReference(newReference: ProxyNode): Boolean {
        Objects.requireNonNull<Any>(newReference, "索引表判断时指定的索引值节点并不为null")
        return Arrays.stream(remoteNodes).anyMatch { newReference == it }
    }

    /** 查找索引表中前X个非重复的索引节点,如果少于要求数,则返回已有的  */
    override fun getFirstFingerTableEntries(i: Int): MutableList<ProxyNode> {
        val result = HashSet<ProxyNode>()
        for (entry in this.remoteNodes) {
            if (entry == null) {
                continue
            }

            result.add(entry)

            if (result.size >= i) {
                break
            }
        }

        return ArrayList(result)
    }

    /** 清除所有代理  */
    override fun clear() {
        Arrays.fill(remoteNodes, null)
    }
}