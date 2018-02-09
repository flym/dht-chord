package io.iflym.dht.node.proxy.socket

import io.iflym.dht.util.loggerFor
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelInboundHandlerAdapter
import io.netty.util.AttributeKey
import java.util.*

/** Created by tanhua(flym) on 7/21/2017.  */
class SocketWatchdog(private val socketProxy: SocketProxy) : ChannelInboundHandlerAdapter() {

    @Throws(Exception::class)
    override fun channelInactive(ctx: ChannelHandlerContext) {

        val proxy = ctx.channel().attr(KEY_CLIENT).getAndSet(null)
        if (proxy == null)
            log.debug("通道断开:{}", ctx.channel())
        else
            log.debug("通道断开:{},客户端:{}", ctx.channel(), proxy.nodeId)

        //仅原来是有效连接的情况下,才重连
        if (proxy != null && !proxy.isServerDown) {
            SocketProxy.asyncGroup.execute { proxy.connect() }
        }

        super.channelInactive(ctx)
    }

    @Throws(Exception::class)
    override fun channelActive(ctx: ChannelHandlerContext) {
        ctx.channel().attr(KEY_CLIENT).set(socketProxy)
        super.channelActive(ctx)
    }

    @Suppress("OverridingDeprecatedMember", "DEPRECATION")
    @Throws(Exception::class)
    override fun exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
        val client = ctx.channel().attr(KEY_CLIENT).get()
        log.debug("发生了异常:{},客户端:{},准备关闭此通道", cause.message, Optional.ofNullable(client).map({ t -> t.nodeId }).orElse(null))

        ctx.channel().close()

        super.exceptionCaught(ctx, cause)
    }

    companion object {
        private val KEY_CLIENT = AttributeKey.valueOf<SocketProxy>("proxyClient")
        private val log = loggerFor<SocketWatchdog>()
    }
}
