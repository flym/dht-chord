package io.iflym.dht.node.proxy.socket

import io.iflym.dht.node.proxy.socket.data.Request
import io.iflym.dht.node.proxy.socket.data.Response
import org.slf4j.LoggerFactory

class RequestInvocation(
        private val invocationChannel: InvocationChannel,
        /** 当前线程需要处理的请求信息  */
        private val request: Request
) : Runnable {

    fun start() {
        invocationChannel.endpoint.scheduleInvocation(this)
        log.debug("处理请求已经加入到处理队列中:{}", request)
    }

    /** 实际处理一次请求  */
    override fun run() {
        val method = this.request.method
        log.debug("准备执行请求方法:{}", method)
        val result: Any?
        try {
            result = invocationChannel.invokeMethod(method, *this.request.parameters)
        } catch (e: Exception) {
            log.error("执行请求出错:{}", e.message, e)
            sendFailureResponse(e, "执行请求时出错,错误信息:" + e.message)
            return
        }

        sendSuccessResponse(result)
        log.debug("请求方法执行完成.:{}", method)
    }

    /** 发送成功信息  */
    private fun sendSuccessResponse(result: Any?) {
        val response = Response.build<Any>(Response.REQUEST_SUCCESSFUL, request.method, this.request.replyWith)
        response.result = result
        invocationChannel.writeAndFlush(response)
    }

    /** 发送失败信息  */
    private fun sendFailureResponse(t: Throwable, failure: String) {
        log.debug("准备发送失败响应信息,原因为:{}", failure)
        val failureResponse = Response.build<Any>(Response.REQUEST_FAILED, request.method, request.replyWith)
        failureResponse.failureReason = failure
        failureResponse.throwable = t

        invocationChannel.writeAndFlush(failureResponse)
    }

    override fun toString(): String {
        return "[Invocation of " + this.request.method +
                "] Request: " + this.request
    }

    companion object {
        private val log = LoggerFactory.getLogger(RequestInvocation::class.java)
    }
}