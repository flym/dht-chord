package io.iflym.dht.node.proxy.socket

import com.google.common.collect.Sets
import io.iflym.dht.net.Endpoint
import io.iflym.dht.node.proxy.socket.data.Request
import io.iflym.dht.node.proxy.socket.protocal.JsonDecoder
import io.iflym.dht.node.proxy.socket.protocal.JsonEncoder
import io.iflym.dht.model.Url
import io.iflym.dht.node.persistent.PersistentNode
import io.iflym.dht.util.loggerFor
import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.*
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import java.util.function.Consumer

/**
 * 针对一个chord节点对外提供服务的socket服务端,实现了基于socket server/client协议的处理方式
 * 通过接收由[SocketProxy]发送的请求处理, 根据不同的网络连接分发请求逻辑并进行响应
 *
 *
 * 此服务端当前仅需要接收不同的网络连接,在连接建立之后,交由后面的单个连接处理线程来负责后续请求接收和处理
 *
 * @author flym
 */
class SocketEndpoint(node: PersistentNode, url: Url) : Endpoint(node, url) {
    /** 当前已连接中的所有通道接收器  */
    private val allChannelSet = Sets.newHashSet<SocketChannel>()

    /** 相应的服务端  */
    private var server: ServerBootstrap? = null
    /** 服务端通道  */
    private var channel: Channel? = null

    /**
     * 执行所有单次请求/响应处理逻辑的线程池
     * [InvocationChannel]
     */
    private val invocationExecutor = InvocationChannel.createInvocationThreadPool()

    override fun openConnections() {
        val port = this.url.port
        try {
            log.debug("准备打开对外端口:{}", port)

            val workerGroup = NioEventLoopGroup()
            server = ServerBootstrap()
            server!!.group(workerGroup, workerGroup)
                    .channel(NioServerSocketChannel::class.java)
                    .childHandler(object : ChannelInitializer<SocketChannel>() {
                        @Throws(Exception::class)
                        override fun initChannel(ch: SocketChannel) {
                            val invocationChannel = InvocationChannel(this@SocketEndpoint, ch)
                            register(invocationChannel)
                            ch.attr(InvocationChannel.ATTR_INVOCATION).set(invocationChannel)
                            allChannelSet.add(ch)

                            val line = ch.pipeline()
                            line.addLast(JsonDecoder())
                            line.addLast(object : SimpleChannelInboundHandler<Any>() {
                                @Throws(Exception::class)
                                override fun channelRead0(channelHandlerContext: ChannelHandlerContext, obj: Any) {
                                    invocationChannel.createNewRequestHandler(obj as Request).start()
                                }

                                @Throws(Exception::class)
                                override fun channelInactive(ctx: ChannelHandlerContext) {
                                    super.channelInactive(ctx)
                                    deregister(invocationChannel)
                                }
                            })
                            ch.pipeline().addLast(JsonEncoder())
                        }
                    })
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)

            println("http server 启动,准备接收数据")

            val f = server!!.bind(port).sync()
            this.channel = f.channel()

            this.setState(LISTENING)

            SocketEndpoint.log.debug("已启动相应的对外服务端")
        } catch (e: Exception) {
            throw RuntimeException("打开对外监听服务失败,端口:" + port + ",原因:" + e.message, e)
        }
    }

    override fun entriesAcceptable() {
        //nothing to do
    }

    override fun doDisconnect() {
        this.invocationExecutor.shutdownNow()

        Sets.newHashSet(allChannelSet).forEach(Consumer<SocketChannel> { this.removeChannel(it) })
        channel!!.close()
        server!!.config().group().shutdownGracefully()
        server!!.config().childGroup().shutdownGracefully()
    }

    /** 调度执行相应的单次请求处理线程  */
    internal fun scheduleInvocation(invocationThread: Runnable) {
        log.debug("调度处理任务:{}", invocationThread)
        this.invocationExecutor.execute(invocationThread)

        log.debug("当前队列数:{}", this.invocationExecutor.queue.size)
        log.debug("当前正在处理任务数:{}", this.invocationExecutor.activeCount)
        log.debug("总完成任务数:{}", this.invocationExecutor.completedTaskCount)
    }

    /** 某个请求接收器需要关闭,将其移除掉  */
    private fun removeChannel(socketChannel: SocketChannel) {
        val invocationChannel = socketChannel.attr(InvocationChannel.ATTR_INVOCATION).get()
        invocationChannel?.disconnect()

        allChannelSet.remove(socketChannel)
    }

    companion object {
        val log = loggerFor<SocketEndpoint>()
    }
}
