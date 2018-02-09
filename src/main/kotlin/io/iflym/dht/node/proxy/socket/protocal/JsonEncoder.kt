package io.iflym.dht.node.proxy.socket.protocal

import io.iflym.dht.util.JsonUtils
import io.netty.buffer.ByteBuf
import io.netty.buffer.ByteBufOutputStream
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.MessageToByteEncoder

/**
 * 实现简单的json编码协议
 * Created by tanhua(flym) on 7/21/2017.
 */
class JsonEncoder : MessageToByteEncoder<Any>() {

    @Throws(Exception::class)
    override fun encode(ctx: ChannelHandlerContext, msg: Any, out: ByteBuf) {
        val startIdx = out.writerIndex()

        val bout = ByteBufOutputStream(out)

        bout.write(LENGTH_PLACEHOLDER)
        val bytes = JsonUtils.toJson(msg)
        bout.write(bytes)

        val endIdx = out.writerIndex()

        out.setInt(startIdx, endIdx - startIdx - 4)
        bout.flush()
    }

    companion object {
        private val LENGTH_PLACEHOLDER = ByteArray(4)
    }
}
