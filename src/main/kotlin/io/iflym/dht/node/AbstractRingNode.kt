package io.iflym.dht.node

import io.iflym.dht.model.Id
import io.iflym.dht.service.ServiceStatus
import io.iflym.dht.util.loggerFor


abstract class AbstractRingNode : RingNode {
    override var serviceStatus = ServiceStatus.UN_INIT

    override var notifiable: Boolean = true

    private fun verifyNotifyDepth(invokingNodeIdList: List<Id>): Boolean {
        if (invokingNodeIdList.contains(nodeId) || invokingNodeIdList.size >= DEPTH_MAX) {
            log.warn("通知调用链已经形成环型调用，或者是超过最大深度，将不再允许传递调用。传递节点列表:{},当前节点:{}", invokingNodeIdList, nodeId)
            return false
        }

        return true
    }

    protected fun <T> withinDepth(invokingNodeIdList: List<Id>, invoking: () -> T?): T? {
        return if (verifyNotifyDepth(invokingNodeIdList)) invoking() else null
    }

    companion object {
        private val log = loggerFor<AbstractRingNode>()
        /** 支持的最大传递深度 */
        private const val DEPTH_MAX = 3
    }

}