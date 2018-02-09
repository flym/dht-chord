package io.iflym.dht.task

import com.google.common.collect.Lists
import io.iflym.dht.data.PreAndSuccessors
import io.iflym.dht.exception.CommunicationException
import io.iflym.dht.model.Entry
import io.iflym.dht.model.Id
import io.iflym.dht.node.RingNode
import io.iflym.dht.reference.References
import org.slf4j.LoggerFactory

/**
 * 用于修复当前直接后缀节点的引用及数据存储
 *
 * @author flym
 */
class DirectSuccessorTask(
        /** 用于维护任务的当前网络节点  */
        private val current: RingNode,
        /** 当前网络节点的引用表  */
        private val references: References,
        private val storageData: Boolean) : Runnable {

    @Suppress("UNCHECKED_CAST")
    override fun run() {
        try {
            if (!current.isRunning())
                return

            log.debug("后缀维护任务准备运行")

            //先移除自己后缀列表中已下线的节点
            Lists.newArrayList(this.references.successorList().successorList).asSequence()
                    .filter { it.isServerDown }
                    .forEach { this.references.removeReference(it) }

            // determine successor
            val directSuccessor = this.references.successorList().directSuccessor
            if (directSuccessor == null) {
                log.debug("当前节点没有后缀,不需要维护")
                return
            }

            //整个逻辑先拿到自己的后缀信息,通过判断引用来查看当前是否是后缀的前缀,如果不是,再进行相应数据维护

            var value: PreAndSuccessors
            try {
                value = directSuccessor.notifyFindValidPreAndSuccessorsWithHint(this.current)
            } catch (e: CommunicationException) {
                log.debug("在获取后缀引用信息时出现通信异常,将移除此后缀节点.后缀节点:{},错误信息:{}", directSuccessor.nodeId, e.message, e)
                this.references.removeReference(directSuccessor)
                return
            }

            //仅在支持数据存储时才处理
            val pre = value.pre
            if (storageData && pre != null) {
                //后缀的前缀与当前并不一样,则认为应该由当前节点来存储相应的数据,那么进行数据迁移
                //同时这里要求,仅当前节点为以下场景时才进行数据迁移 postPre < current < successor,即当前节点在中间时才处理
                if (current.nodeId != pre.nodeId && current.nodeId.isInInterval(pre.nodeId, directSuccessor.nodeId)) {
                    val valueAndEntries = directSuccessor.notifyFindValidPreAndSuccessorsAndTransferDataWithHint(this.current)
                    value = PreAndSuccessors.of(valueAndEntries)

                    current.notifyInsertEntriees(valueAndEntries.entries as Map<Id, Entry<Any>>)
                }
            }

            //更新自己的后缀列表
            references.addReferences(value.successorList)

            log.info("执行后缀维护任务成功")
        } catch (e: Exception) {
            log.warn("在执行后缀维护任务时出现异常:{}", e.message, e)
        }
    }

    companion object {
        val log = LoggerFactory.getLogger(DirectSuccessorTask::class.java)!!
    }
}