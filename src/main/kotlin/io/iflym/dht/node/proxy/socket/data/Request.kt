package io.iflym.dht.node.proxy.socket.data

import java.util.*

/**
 * 描述在socket通信中进行网络通信时的请求信息.
 *
 *
 * 一个request通过socketProxy进行数据发送, 然后远端的socketEndpoint接收相应的请求数据,并返回一个response信息
 *
 * @author flym
 */
class Request : Message() {
    /** 请求方法定义值  */
    lateinit var method: MethodValue

    /**
     * 用于描述双方交互时表示请求/响应的惟一标识符,即通信惟一描述符.
     * 请求时发送此标识符,响应时也响应此标识符,请求端通过标识符进行响应匹配
     */
    lateinit var replyWith: String

    /**
     * 请求的参数信息
     * 请求参数需要与 type中所对应的请求方法所接收的参数定义(类型及数量)相一致
     */
    lateinit var parameters: Array<Any>

    override fun toString(): String {
        return "Request(method=$method, replyWith='$replyWith', parameters=${Arrays.toString(parameters)})"
    }
}
