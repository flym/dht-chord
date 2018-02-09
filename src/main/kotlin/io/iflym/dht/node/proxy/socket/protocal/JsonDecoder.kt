package io.iflym.dht.node.proxy.socket.protocal

import io.iflym.dht.util.JsonUtils
import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.LengthFieldBasedFrameDecoder

/**
 * 实现简单的json解码器
 * Created by tanhua(flym) on 7/21/2017.
 */
class JsonDecoder : LengthFieldBasedFrameDecoder(
        //最大对象10M
        10 * 1024 * 1024, 0, 4, 0, 4
) {
    @Throws(Exception::class)
    override fun decode(ctx: ChannelHandlerContext, `in`: ByteBuf): Any? {
        val frame = (super.decode(ctx, `in`) ?: return null) as ByteBuf

        val bytes = ByteArray(frame.readableBytes())
        frame.readBytes(bytes)

        try {
            return JsonUtils.parse<Any>(bytes)
        } finally {
            frame.release()
        }
    }
}