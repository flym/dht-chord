package io.iflym.dht.node.proxy.socket.data

import io.iflym.dht.model.Id
import io.iflym.dht.model.Url
import io.iflym.dht.node.RingNode

/**
 * 描述一个远端的节点信息.即一个节点通过地址和id来惟一确定节点信息
 *
 * @author flym
 */
data class RemoteNodeInfo(
        /** 节点id  */
        val nodeId: Id,

        /** 节点地址信息  */
        val nodeUrl: Url,

        /** 此节点是否可通知访问 */
        val notifiable: Boolean
) {
    companion object {
        fun of(node: RingNode): RemoteNodeInfo {
            return RemoteNodeInfo(node.nodeId, node.nodeUrl, node.notifiable)
        }
    }
}
