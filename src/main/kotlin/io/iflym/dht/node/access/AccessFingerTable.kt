package io.iflym.dht.node.access

import io.iflym.dht.model.Id
import io.iflym.dht.reference.AbstractFingerTable
import io.iflym.dht.reference.References

/**
 * 维护当前节点的索引表,用于快速定位数据查找和处理
 *
 * @author flym
 */
internal class AccessFingerTable(localID: Id, references: References) : AbstractFingerTable(localID, references)