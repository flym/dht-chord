package io.iflym.dht.node.proxy.socket.data

import io.iflym.dht.data.PreAndSuccessors


/**
 *  前缀以及后缀列表(用于数据传输)
 *  数据对象，封装相应的数据信息
 *
 *  @author flym
 *
 * */
data class RemotePreAndSuccessors(
        /** 前缀 */
        val pre: RemoteNodeInfo?,

        /** 后缀集 */
        val successorList: List<RemoteNodeInfo>
) {
    companion object {
        val empty = RemotePreAndSuccessors(null, emptyList())

        fun of(value: PreAndSuccessors): RemotePreAndSuccessors = RemotePreAndSuccessors(
                value.pre?.let { RemoteNodeInfo.Companion.of(it) },
                value.successorList.map { RemoteNodeInfo.Companion.of(it) })
    }
}
