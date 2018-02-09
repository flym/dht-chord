package io.iflym.dht.model

import com.alibaba.fastjson.annotation.JSONField
import io.iflym.dht.util.Asserts
import io.iflym.dht.util.ByteUtils
import io.iflym.dht.util.HashFunction
import java.util.*

/**
 * 用于一个对象的惟一标识信息,即给每个对象标识一个惟一值,包括节点,数据
 * 惟一标识不可变,并且所有的标识信息的长度都是一样的,不同长度标识不可比较
 *
 * @author flym
 */
data class Id constructor(
        /** 原始的字节数组表示形式  */
        val id: ByteArray,
        /** 用于形象表示此id值的名称,即在显示时不再使用字节的处理形式,而是直接显示此语义名  */
        val semanticName: String? = null
) : Comparable<Id> {

    init {
        val bytesLength = HashFunction.LENGTH_OF_ID_IN_BYTES
        Asserts.assertTrue(id.size == bytesLength, "要求id长度必须为:{}位，当前位:{}", bytesLength, id.size)
    }

    /** 获取这个字节数据的长度值(以位计算,1个字节8位)  */
    val length: Int
        @JSONField(serialize = false)
        get() = this.id.size * 8

    private val stringRepresentation by lazy { semanticName ?: this.toHexString(numberOfDisplayedBytes) }

    override fun toString(): String = stringRepresentation

    /** 返回当前标识前X个字节的16进制显示字符串  */
    private fun toHexString(numberOfBytes: Int): String {
        val displayBytes = Math.max(1, Math.min(numberOfBytes, this.id.size))

        val result = StringBuilder()
        for (i in 0 until displayBytes) {
            val block = Integer.toHexString(this.id[i].toInt() and 0xff).toUpperCase()
            if (block.length < 2) {
                result.append('0')
            }

            result.append(block)
        }
        return result.toString()
    }

    /** 返回当前标识的16进制全量字符串  */
    fun toHexString(): String = this.toHexString(this.id.size)

    /**
     * 在当前字节数据上增加2的N次方值,并返回新的字节值
     *
     * @param powerOfTwo 2的N次方 N必须在[0,length-1]之间
     */
    fun addPowerOfTwo(powerOfTwo: Int): Id {
        if (powerOfTwo < 0 || powerOfTwo >= this.id.size * 8) {
            throw IllegalArgumentException("指数必须在0到length-1之间:" + powerOfTwo)
        }

        val copy = ByteArray(this.id.size)
        System.arraycopy(this.id, 0, copy, 0, this.id.size)

        var indexOfByte = this.id.size - 1 - powerOfTwo / 8
        val toAdd = byteArrayOf(1, 2, 4, 8, 16, 32, 64, -128)
        var valueToAdd = toAdd[powerOfTwo % 8]
        var oldValue: Byte

        do {
            oldValue = copy[indexOfByte]
            copy[indexOfByte] = (copy[indexOfByte] + valueToAdd).toByte()

            valueToAdd = 1
        } while (oldValue < 0 && copy[indexOfByte] >= 0 && indexOfByte-- > 0)

        return Id(copy)
    }

    /**
     * 对比2个数据值, 以提供基本的比较判断值
     * 如果2个值的长度不同,则报错
     */
    @Throws(ClassCastException::class)
    override fun compareTo(other: Id): Int {
        if (this.length != other.length) {
            throw IllegalArgumentException("两个比较数据值长度不一致,不可进行比较.当前长度:" + id.size + ",比较值长度:" + other.id.size)
        }

        return ByteUtils.compareBitTo(id, other.id)
    }

    /**
     * 判断当前字节值是否是指定区间之间
     * 如用于如存储数据时判断此数据id是否在2个节点之间,以判断是否存储在此节点上
     *
     * @param fromID 下界值
     * @param toID   上界值
     */
    fun isInInterval(fromID: Id, toID: Id): Boolean {
        // 如果上界和下界均相同,则要求此值必须不能与比较值相同,即只要不相同,则就是在其间.即要求存储数据应该不与节点id相同
        if (fromID == toID) {
            return this != fromID
        }

        // 如果下界比上界小,则认为是从 0 -> 下界 -> 上界 -> 最大值 的一个范围比较顺序
        return if (fromID < toID) {
            this > fromID && this < toID
        } else {
            // 如果下界比上界大,则认为是从 上界 -> 最大值 -> 0 -> 下界 的一个范围比较顺序
            //2种情况,当前值落在 上界到max之间 或者 0到下界之间
            fromID != MAX && this > fromID && this <= MAX || MIN != toID && this >= MIN && this < toID
        }
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as Id

        if (!Arrays.equals(id, other.id)) return false

        return true
    }

    override fun hashCode(): Int {
        return Arrays.hashCode(id)
    }

    companion object {
        //20位算法
        private const val LENGTH = 20
        val MIN = Id(ByteArray(LENGTH))
        val MAX = Id(ByteArray(LENGTH).also { it.fill(0xFF.toByte()) })

        /** 表示当前标识在进行显示时需要显示的字节数,即并不是每个字节都是要toString显示的,这里表示只显示前X个字节信息  */
        private var numberOfDisplayedBytes = Integer.MAX_VALUE

        init {
            val numberProperty = System.getProperty(Id::class.java.name + ".number.of.displayed.bytes")

            if (numberProperty != null && numberProperty.isNotEmpty()) {
                numberOfDisplayedBytes = Integer.parseInt(numberProperty)
            }
        }

        /** 使用语义字符串构建出id值对象  */
        fun of(semanticName: String): Id {
            return of(semanticName.toByteArray(), semanticName)
        }

        /** 使用业务字节值信息以及指定的语义字符串构建id值对象  */
        fun of(bytes: ByteArray, semanticName: String? = null): Id {
            return Id(HashFunction.hash(bytes), semanticName)
        }
    }
}