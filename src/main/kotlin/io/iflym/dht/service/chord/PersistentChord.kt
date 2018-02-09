package io.iflym.dht.service.chord

import io.iflym.dht.model.Id
import io.iflym.dht.node.persistent.PersistentNode
import io.iflym.dht.node.Reportable

/**
 * 用于实现一个主要的chord网络节点语义,即实现同步式chord网络交互,异步式网络交互以及数据存储的详细处理
 *
 * @author flym
 */
class PersistentChord : AbstractChord() {
    override fun initCreate() {
        //用于通信的节点信息
        this.localNode = PersistentNode(id, url)

        //设置必要的参数
        thisNode().asyncExecutor = asyncExecutor

        thisNode().init()
    }

    override fun postCreate() {
        //已构建成功, 该节点可以接收数据请求了
        thisNode().start()
        thisNode().acceptEntries()
    }

    private fun thisNode(): PersistentNode {
        return localNode as PersistentNode
    }

    override fun <T> tryGetReplicaEntry(id: Id): T? {
        //本地存储中可能有相应的副本数据,因此优先找副本数据
        val entry = thisNode().getReplicaEntry<T>(id)
        if (entry != null) {
            logger.debug("在副本中找到了相应的数据.节点:{},id:{}", localNode, id)
            return entry.value
        }

        return null
    }

    override fun reportedNode(): Reportable {
        return localNode as Reportable
    }
}