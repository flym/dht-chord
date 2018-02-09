package io.iflym.dht.node.proxy.socket.data

/**
 * 描述一个通过网络进行传输的数据信息
 *
 * @author flym
 */
abstract class Message {
    /** 当前数据发送时间  */
    var timeStamp: Long = 0
}
