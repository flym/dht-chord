package io.iflym.dht.service.chord

import io.iflym.dht.node.Reportable
import io.iflym.dht.node.access.AccessNode

/**
 * 用于描述一个专门用于访问chord网络的接入点,其在chord网络中仅用于一个接入点,以提供对外访问chord网络能力
 * 但是自己并不承担专门的存储义务,即意味着它不会被其它的网络节点看到,仅自己可见
 *
 * @author flym
 */
class AccessChord : AbstractChord() {
    private fun thisNode(): AccessNode = localNode as AccessNode

    override fun initCreate() {
        val accessNode = AccessNode(id, url)
        //用于通信的节点信息
        this.localNode = accessNode

        accessNode.init()
    }

    override fun postCreate() {
        thisNode().start()
    }

    override fun reportedNode(): Reportable? {
        return null
    }

}