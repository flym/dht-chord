package io.iflym.dht.node.access

import io.iflym.dht.data.PreAndSucccessorsAndEntries
import io.iflym.dht.data.PreAndSuccessors
import io.iflym.dht.exception.CommunicationException
import io.iflym.dht.exception.ServiceException
import io.iflym.dht.model.Entry
import io.iflym.dht.model.Id
import io.iflym.dht.model.Key
import io.iflym.dht.model.Url
import io.iflym.dht.node.AbstractRingNode
import io.iflym.dht.node.ProxyNode
import io.iflym.dht.node.ReferencedNode
import io.iflym.dht.node.RingNode
import io.iflym.dht.service.ServiceStatus
import io.iflym.dht.service.chord.AbstractChord.Companion.FIX_FINGER_TASK_INTERVAL
import io.iflym.dht.service.chord.AbstractChord.Companion.FIX_FINGER_TASK_START
import io.iflym.dht.service.chord.AbstractChord.Companion.STABILIZE_TASK_INTERVAL
import io.iflym.dht.service.chord.AbstractChord.Companion.STABILIZE_TASK_START
import io.iflym.dht.task.DirectSuccessorTask
import io.iflym.dht.task.FixFingerTask
import io.iflym.dht.util.ThreadFactoryUtils
import io.iflym.dht.util.loggerFor
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.TimeUnit

/**
 * 用于描述一个访问的入口节点
 *
 * @author flym
 */
class AccessNode
internal constructor(override val nodeId: Id, override val nodeUrl: Url
) : AbstractRingNode(), ReferencedNode {

    /** 访问节点不可访问 */
    override var notifiable: Boolean = false

    /** 本地维护任务线程执行器  */
    private val maintenanceTasks: ScheduledExecutorService

    override val references = AccessReferences(this)

    init {
        //维护任务有2个, 后缀,索引表
        this.maintenanceTasks = ScheduledThreadPoolExecutor(2, ThreadFactoryUtils.build("AccessNodeMtTask-$nodeId-%d"))
    }

    override fun disconnect() {
        serviceStatus = ServiceStatus.STOPPED

        this.maintenanceTasks.shutdownNow()
    }

    override fun notifyFindValidNode(id: Id): RingNode? {
        return references.findSuccessorWithinReferences(id)
    }

    override fun notifyFindValidNode4Get(id: Id): RingNode? {
        return references.findSuccessorWithinReferences4Get(id)
    }

    override fun notifyFindValidPreAndSuccessorsWithHint(potentialPredecessor: RingNode): PreAndSuccessors {
        //nothing to do
        return PreAndSuccessors.empty
    }

    @Throws(CommunicationException::class)
    override fun notifyFindValidPreAndSuccessors(): PreAndSuccessors {
        //nothing to do
        return PreAndSuccessors.empty
    }

    @Throws(CommunicationException::class)
    override fun notifyFindValidPreAndSuccessorsAndTransferDataWithHint(potentialPredecessor: RingNode): PreAndSucccessorsAndEntries {
        //nothing to do
        return PreAndSucccessorsAndEntries.empty
    }

    @Throws(CommunicationException::class)
    override fun notifyChangeStoreToReplicas(potentialPredecessor: RingNode) {
        //nothing to do
    }

    @Throws(CommunicationException::class)
    override fun <T> insertEntry(key: Key, toInsert: T) {
        //nothing to do
    }

    override fun <T> notifyInsertReplicas(insertReplicas: Map<Id, Entry<T>>) {
        //nothing to do
    }

    @Throws(CommunicationException::class)
    override fun deleteEntry(entryToRemove: Key) {
        //nothing to do
    }

    override fun notifyDeleteReplicas(sendingNode: Id, replicasToRemove: Set<Id>) {
        //nothing to do
    }

    @Throws(CommunicationException::class)
    override fun <T> getEntry(key: Key): T? {
        //nothing to do
        return null
    }

    override fun leaveNetwork(predecessor: RingNode) {
        //nothing to do
    }

    override fun init() {
        //nothing to do
    }

    override fun <T> notifyGetEntriesInterval(fromId: Id, toId: Id): List<Entry<T>> {
        return emptyList()
    }

    override fun <T> notifyInsertEntriees(insertEntries: Map<Id, Entry<T>>) {
        //nothing to do
    }

    override fun <T> notifyPreInsertEntry(key: Key, toInsert: T, invokingNodeIdList: List<Id>) {
        //nothing to do
    }

    override fun notifyPreDeleteEntry(entryToRemove: Key, invokingNodeIdList: List<Id>) {
        //nothing to do
    }

    override fun <T> notifyPreGetEntry(key: Key, invokingNodeIdList: List<Id>): T? {
        //nothing to do
        return null
    }

    override fun start() {
        serviceStatus = ServiceStatus.RUNNING

        startMaintainTask()
    }

    private fun startMaintainTask() {
        //索引表
        this.maintenanceTasks.scheduleWithFixedDelay(FixFingerTask(this, nodeId, references),
                FIX_FINGER_TASK_START.toLong(), FIX_FINGER_TASK_INTERVAL.toLong(), TimeUnit.SECONDS)

        //后缀维护
        this.maintenanceTasks.scheduleWithFixedDelay(DirectSuccessorTask(this, references, false),
                STABILIZE_TASK_START.toLong(), STABILIZE_TASK_INTERVAL.toLong(), TimeUnit.SECONDS)
    }

    override fun leaveMayNotifyOthers() {
        //nothing to do
    }

    @Throws(ServiceException::class)
    override fun processRefsInJoin(bootstrapNode: ProxyNode, newSuccessor: RingNode) {
        val value: PreAndSuccessors
        try {
            value = newSuccessor.notifyFindValidPreAndSuccessors()
        } catch (e: CommunicationException) {
            throw ServiceException("加入网络时获取相应的引用信息失败:" + e.message, e)
        }

        //添加前缀当作引用信息
        value.pre?.let { references.addReferences(listOf(it)) }

        // 将这些新的引用后缀信息,都加入到当前引用信息中,作为后缀来处理
        references.addReferences(value.successorList)
    }

    companion object {
        val logger = loggerFor<AccessNode>()
    }
}