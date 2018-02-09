package io.iflym.dht.util

import java.security.MessageDigest

/**
 * 提供一个计算key,url的hash计算功能,目的在于计算所有数据均能返回可比较,并且长度一致的id值信息
 *
 * @author flym
 */
object HashFunction {
    private val digest = MessageDigest.getInstance("SHA-1")
    /** 返回由此摘要函数生成的id值的长度信息(按字节计算)  */
    val LENGTH_OF_ID_IN_BYTES = digest.digestLength

    fun hash(bytes: ByteArray): ByteArray {
        //这里为保证正确性, 对摘要函数计算采用锁机制处理
        synchronized(digest) {
            digest.reset()
            digest.update(bytes)
            return digest.digest() as ByteArray
        }
    }
}