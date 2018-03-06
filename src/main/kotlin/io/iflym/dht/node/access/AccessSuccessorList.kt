package io.iflym.dht.node.access

import io.iflym.dht.node.RingNode
import io.iflym.dht.reference.AbstractSuccessorList
import io.iflym.dht.reference.References

/** Created by flym on 7/26/2017.  */
class AccessSuccessorList(node: RingNode, numberOfEntries: Int, references: References) : AbstractSuccessorList(node, numberOfEntries, references)
