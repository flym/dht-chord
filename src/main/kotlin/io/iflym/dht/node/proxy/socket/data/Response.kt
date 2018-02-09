package io.iflym.dht.node.proxy.socket.data

import com.alibaba.fastjson.annotation.JSONField

/**
 * 描述socket协议交互中的响应信息
 *
 * @author flym
 */
class Response<T> : Message() {

    /** 请求失败原因  */
    var failureReason: String? = null
        /** 设置失败信息  */
        @JSONField(serialize = false)
        set(reason) {
            this.status = REQUEST_FAILED
            field = reason
        }

    /** 成功响应结果  */
    var result: T? = null

    /** 当前请求所对应访问的方法  */
    private var methodValue: MethodValue? = null

    /** 当前响应状态值  */
    private var status = REQUEST_SUCCESSFUL

    /** 惟一通信标识符  */
    var inReplyTo: String? = null

    /** 如果请求出错了,则在方法调用过程中所产生的错误堆栈信息  */
    var throwable: Throwable? = null

    /** 当前响应是否失败了  */
    val isFailureResponse: Boolean
        @JSONField(serialize = false)
        get() = this.status == REQUEST_FAILED

    companion object {
        /** 常量_请求正常,返回数据正常  */
        const val REQUEST_SUCCESSFUL = 1

        /** 常量_请求失败,返回错误信息  */
        const val REQUEST_FAILED = 0

        fun <T> build(status: Int, methodValue: MethodValue, inReplyTo: String): Response<T> {
            val value = Response<T>()
            value.status = status
            value.methodValue = methodValue
            value.inReplyTo = inReplyTo

            return value
        }
    }

    override fun toString(): String {
        return "Response(failureReason=$failureReason, result=$result, methodValue=$methodValue, status=$status, inReplyTo=$inReplyTo, throwable=$throwable)"
    }
}
