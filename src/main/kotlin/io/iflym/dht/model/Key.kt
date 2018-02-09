package io.iflym.dht.model

import java.util.*

/**
 * 用于描述存储在chord网络中的对象的标识信息,即每一个对象都会有一个标识,则标识用于形象化地表示相应的存储内容.
 *
 *
 * 需要注意的是,key和id是不相同的,在chord网络中,id是通过hash计算key而得出的一个比较值,它有固定的长度.通过id可以方便地定位到此key所对应的
 * 存储数据应该放在哪儿,以及在哪儿可以查找到.
 * 可以认为key是存储内容标识,而id用来定位位置和实现可比较性
 *
 * @author flym
 */
data class Key(
        /** 获取此key的字节表示形式,以序列化值信息.此字节可以用于计算id. */
        val bytes: ByteArray,
        /**
         * 获取此key的语义话表示,即可显示的字符串
         * 如果key本身就是字符串,那么语义串可以是就字符串本身.即一个更好的toString形式
         */
        val semanticName: String? = null
) {
    val id by lazy { Id.of(bytes, semanticName) }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as Key

        if (!Arrays.equals(bytes, other.bytes)) return false

        return true
    }

    override fun hashCode(): Int {
        return Arrays.hashCode(bytes)
    }

    override fun toString(): String {
        return "Key(${semanticName ?: Arrays.toString(bytes)})"
    }

    companion object {
        /** 构建对象 */
        fun of(semanticName: String): Key = Key(semanticName.toByteArray(), semanticName)
    }
}