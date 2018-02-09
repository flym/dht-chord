package io.iflym.dht.node.proxy.socket.data

import io.iflym.dht.data.PreAndSucccessorsAndEntries
import io.iflym.dht.model.Entry
import io.iflym.dht.model.Id

/**
 * 描述一个节点想要了解自己需要处理的引用节点以及需要复制的数据集
 * 远程数据传输使用
 *
 * @author flym
 */
data class RemotePreAndSucccessorsAndEntries(
        /** 前缀节点 */
        val pre: RemoteNodeInfo?,
        /** 后缀列表 */
        val successorList: List<RemoteNodeInfo>,

        /** 对响应节点来说需要复制的数据集,如无则emptyMap  */
        val entries: Map<Id, Entry<*>> = emptyMap()
) {
    companion object {
        fun of(value: PreAndSucccessorsAndEntries): RemotePreAndSucccessorsAndEntries = RemotePreAndSucccessorsAndEntries(
                value.pre?.let { RemoteNodeInfo.Companion.of(it) },
                value.successorList.map { RemoteNodeInfo.Companion.of(it) },
                value.entries
        )
    }
}
