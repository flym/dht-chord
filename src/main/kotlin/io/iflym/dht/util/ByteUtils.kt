package io.iflym.dht.util

import com.google.common.primitives.Ints

import java.nio.ByteBuffer

/**
 * 字节数组工具类,用于按原生的字节位进行比较
 * Created by flym on 7/17/2017.
 */
object ByteUtils {
    fun compareBitTo(aKey: ByteArray, bKey: ByteArray): Int {
        val n = Math.min(aKey.size, bKey.size)
        for (i in 0 until n) {
            val cmp = compareBitTo(aKey[i], bKey[i])
            if (cmp != 0)
                return cmp
        }

        return Ints.compare(aKey.size, bKey.size)
    }

    fun compareBitTo(self: ByteBuffer, that: ByteBuffer): Int {
        val n = self.position() + Math.min(self.remaining(), that.remaining())
        var i = self.position()
        var j = that.position()
        while (i < n) {
            val cmp = compareBitTo(self.get(i), that.get(j))
            if (cmp != 0)
                return cmp
            i++
            j++
        }
        return self.remaining() - that.remaining()
    }

    private fun compareBitTo(a: Byte, b: Byte): Int {
        val minusA = (a - 128).toByte()
        val minusB = (b - 128).toByte()

        if (minusA == minusB)
            return 0
        return if (minusA < minusB) -1 else 1
    }

}
