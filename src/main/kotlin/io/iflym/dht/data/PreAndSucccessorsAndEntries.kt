package io.iflym.dht.data

import io.iflym.dht.model.Entry
import io.iflym.dht.model.Id
import io.iflym.dht.node.RingNode

/**
 * 描述一个节点想要了解自己需要处理的引用节点以及需要复制的数据集
 *
 * @author flym
 */
data class PreAndSucccessorsAndEntries(
        /** 前缀节点 */
        val pre: RingNode?,
        /** 后缀列表 */
        val successorList: List<RingNode>,

        /** 对响应节点来说需要复制的数据集,如无则emptyMap  */
        val entries: Map<Id, Entry<*>> = emptyMap()
) {
    companion object {
        val empty = PreAndSucccessorsAndEntries(null, emptyList())
    }
}
