package io.iflym.dht.task

import io.iflym.dht.model.Id
import io.iflym.dht.node.RingNode
import io.iflym.dht.reference.References
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.ThreadLocalRandom

/**
 * 执行维护索引表的周期性任务
 * 即检查自己的索引表,随机性抽查一个位置.再从引用表中重新查找应该处理此位置的网络节点,如果此网络节点并不在自己这里存储,然后更新索引
 *
 * @author flym
 */
class FixFingerTask(
        /** 维护的是哪个节点  */
        private val current: RingNode,
        /** 当前节点的下标  */
        private val localID: Id,
        /** 反向引用的引用表信息  */
        private val references: References) : Runnable {

    /** 随机性的维护索引位置  */
    private val random = ThreadLocalRandom.current()

    /** 用于记录日志的日志对象  */
    private val logger: Logger

    init {
        Objects.requireNonNull<Any>(current, "构建索引表维护任务时维护节点不能为null")
        Objects.requireNonNull<Any>(localID, "构建索引表维护任务时节点id值不能为null")
        Objects.requireNonNull<Any>(references, "构建索引表维护任务时引用表不能为null")

        this.logger = LoggerFactory.getLogger(FixFingerTask::class.java.name + "." + localID)
    }

    override fun run() {
        logger.debug("索引维护任务准备运行")

        try {
            if (!current.isRunning())
                return

            //随机维护一个位置
            val nextFingerToFix = this.random.nextInt(this.localID.length)

            logger.debug("准备维护指定下标处的索引位置,下标:{}", nextFingerToFix)

            val lookForID = localID.addPowerOfTwo(nextFingerToFix)
            val newReference = current.notifyFindValidNode(lookForID)

            //这个找到的处理节点确实找到了,并且不是自己,并且还没有在当前引用表中存在,则加入到引用表中(同时会更新索引表)
            newReference?.let {
                logger.debug("维护索引表时发现了新引用节点,添加之:{} ", it.nodeId)
                references.addReferences(listOf(it))
            }

            logger.info("完成维护索引任务")
        } catch (e: Exception) {
            logger.warn("在执行索引任务时出现异常:{}", e.message, e)
        }

    }
}