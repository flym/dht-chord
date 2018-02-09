package io.iflym.dht.node.persistent

import com.google.common.collect.Lists
import io.iflym.dht.model.Id
import io.iflym.dht.node.ProxyNode
import io.iflym.dht.reference.AbstractFingerTable

/**
 * 维护当前节点的索引表,用于快速定位数据查找和处理
 *
 * @author flym
 */
class PersistentFingerTable(localID: Id, references: PersistentReferences) : AbstractFingerTable(localID, references) {

    private fun thisReferences(): PersistentReferences {
        return references as PersistentReferences
    }

    override fun postRemoveReference(node: ProxyNode) {
        // 因为前面有填坑的,可能导致索引表置空,因此从后缀表中看有不骨可能用来填上相应坑的数据
        val referencesOfSuccessorList = Lists.newArrayList(thisReferences().successorList().successorList)
        //此节点已经移除,不使用此
        referencesOfSuccessorList.remove(node)
        referencesOfSuccessorList.forEach { this.addReference(it) }
    }
}