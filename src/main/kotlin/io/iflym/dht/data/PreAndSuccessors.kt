package io.iflym.dht.data

import io.iflym.dht.node.RingNode


/**
 *  前缀以及后缀列表
 *  数据对象，封装相应的数据信息
 *
 *  @author flym
 *
 * */
data class PreAndSuccessors(
        /** 前缀 */
        val pre: RingNode?,

        /** 后缀集 */
        val successorList: List<RingNode>
) {
    companion object {
        val empty = PreAndSuccessors(null, emptyList())

        fun of(value: PreAndSucccessorsAndEntries): PreAndSuccessors = PreAndSuccessors(value.pre, value.successorList)
    }
}
