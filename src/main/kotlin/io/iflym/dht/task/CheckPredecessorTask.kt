package io.iflym.dht.task

import io.iflym.dht.node.RingNode
import io.iflym.dht.node.persistent.PersistentReferences
import io.iflym.dht.util.loggerFor
import java.util.*

/**
 * 检查自己的前缀是否仍存活,如果不再有效,则将其从自己前缀中移除
 *
 * @author flym
 */
class CheckPredecessorTask(
        /** 当前节点 */
        private val current: RingNode,
        /** 相应的引用表  */
        private val references: PersistentReferences) : Runnable {
    init {
        Objects.requireNonNull(references, "构建前缀维护任务时相应引用不再为null")
    }

    override fun run() {
        try {
            if (!current.isRunning())
                return

            log.debug("检查前缀维护任务开始执行")

            val predecessor = this.references.predecessor() ?: return
            //自己当前无前缀,无需检查

            if (predecessor.isServerDown) {
                log.debug("前缀节点已经下线,准备移除它.")
                this.references.removeReference(predecessor)
                return
            }

            log.info("检查前缀任务已成功完成,前缀:{}", predecessor.nodeId)
        } catch (e: Exception) {
            log.warn("执行前缀检查任务时出现未知异常:{}", e.message, e)
        }
    }

    companion object {
        val log = loggerFor<CheckPredecessorTask>()
    }
}