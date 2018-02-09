package io.iflym.dht.node.persistent

import com.google.common.collect.Sets
import io.iflym.dht.data.PreAndSucccessorsAndEntries
import io.iflym.dht.data.PreAndSuccessors
import io.iflym.dht.exception.CommunicationException
import io.iflym.dht.exception.ServiceException
import io.iflym.dht.model.Entry
import io.iflym.dht.model.Id
import io.iflym.dht.model.Key
import io.iflym.dht.model.Url
import io.iflym.dht.net.Endpoint
import io.iflym.dht.node.*
import io.iflym.dht.service.ServiceStatus
import io.iflym.dht.service.chord.AbstractChord
import io.iflym.dht.storage.DataStorage
import io.iflym.dht.storage.RocksDataStorage
import io.iflym.dht.task.CheckPredecessorTask
import io.iflym.dht.task.DirectSuccessorTask
import io.iflym.dht.task.FixFingerTask
import io.iflym.dht.util.ThreadFactoryUtils
import io.iflym.dht.util.withLock
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.Executor
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReentrantLock

/**
 * 用于实现真实的一个底层数据存储节点， 实际用于数据存储和处理的节点
 *
 * @author flym
 */
class PersistentNode internal constructor(override val nodeId: Id, override val nodeUrl: Url) : AbstractRingNode(), ReferencedNode, ReportedNode, RingNode {

    /** 数据存储  */
    private val dataStorage: DataStorage

    /** 专门存储副本的容器  */
    private val replicaDataStorage: DataStorage

    /** 自己的对外端点服务  */
    private val endpoint: Endpoint = Endpoint.createEndpoint(this, nodeUrl)

    /** 与当前通信节点相关的日志对象  */
    private val logger: Logger = LoggerFactory.getLogger(PersistentNode::class.java.name + "." + nodeId.toString())

    /** 引用表信息  */
    override val references: PersistentReferences = PersistentReferences(this)

    /** 引用在chord中的异步处理线程池  */
    lateinit var asyncExecutor: Executor

    /** 用于在notify各项处理中保证同步运行机制的锁信息,避免出现单点不一致的情况  */
    private val notifyLock: Lock = ReentrantLock(true)

    /** 本地维护任务线程执行器  */
    private val maintenanceTasks: ScheduledExecutorService

    init {
        //数据存储
        this.dataStorage = RocksDataStorage.instance
        //副本存储
        this.replicaDataStorage = RocksDataStorage.replicaInstance

        dataStorage.addNodeUrl(nodeUrl)
        replicaDataStorage.addNodeUrl(nodeUrl)

        //维护类任务
        //维护任务有3个, 后缀,前缀,索引表
        this.maintenanceTasks = ScheduledThreadPoolExecutor(3, ThreadFactoryUtils.build("NodeMtTask-$nodeId-%d"))
    }

    override fun init() {
        //在初始化过程中即开始监听服务，以方便其它节点访问到节点，以在整个集群启动过程中均可以看到对方
        //准备开始对外端点服务,并开始监听相应的访问请求
        this.endpoint.listen()
    }

    override fun start() {
        serviceStatus = ServiceStatus.RUNNING

        startMaintainTask()
    }

    private fun startMaintainTask() {
        //后缀维护
        maintenanceTasks.scheduleWithFixedDelay(DirectSuccessorTask(this, references, true),
                AbstractChord.STABILIZE_TASK_START.toLong(), AbstractChord.STABILIZE_TASK_INTERVAL.toLong(), TimeUnit.SECONDS)

        //索引表
        maintenanceTasks.scheduleWithFixedDelay(FixFingerTask(this, nodeId, references),
                AbstractChord.FIX_FINGER_TASK_START.toLong(), AbstractChord.FIX_FINGER_TASK_INTERVAL.toLong(), TimeUnit.SECONDS)

        //前缀维护
        maintenanceTasks.scheduleWithFixedDelay(CheckPredecessorTask(this, references),
                AbstractChord.CHECK_PREDECESSOR_TASK_START.toLong(), AbstractChord.CHECK_PREDECESSOR_TASK_INTERVAL.toLong(), TimeUnit.SECONDS)
    }

    /** 标明当前节点可以接收其它节点发送的数据访问类请求了  */
    internal fun acceptEntries() {
        this.endpoint.acceptEntries()
    }

    override fun disconnect() {
        serviceStatus = ServiceStatus.STOPPED

        endpoint.disconnect()
        maintenanceTasks.shutdownNow()
        references.clearAndDisconnect()
    }

    /** 指定id是否应该由自己来处理 */
    private fun shouldSelfProcess(id: Id): Boolean {
        val pre = references.predecessor() ?: return false
        return id.isInInterval(pre.nodeId, nodeId)
    }

    override fun notifyFindValidNode(id: Id): RingNode? {
        Objects.requireNonNull<Any>(id, "查询处理响应节点时key值不能为null")

        if (shouldSelfProcess(id))
            return this

        //如果本身就由自己处理，则直接返回自己
        val value = findValidDirectSuccessor(id) ?: references.findSuccessorWithinReferences(id)

        if (value === this) {
            logger.debug("引用表中已没有合适的后缀节点可以处理,因此最终返回自己")
        }

        return value
    }

    private fun findValidDirectSuccessor(key: Id): RingNode? {
        val successor = references.successorList().directSuccessor
        //整个网络中就只有当前节点,因此返回自己
        if (successor == null) {
            logger.debug("网络中仅此单个节点,返回自己", nodeId)
            return this
        }

        if (!successor.isCurrentAlive) {
            logger.debug("后缀节点不可用，将直接返回null.后缀:{}", successor)
            return null
        }

        //key值恰好在当前节点和后缀节点之间,则数据由后缀节点处理
        val successorNodeID = successor.nodeId
        if (key.isInInterval(nodeId, successorNodeID) || key == successorNodeID) {
            logger.debug("要查找的数据在当前节点和后缀节点之间,返回后续节点.后缀值:{},查找值:{}", successorNodeID, key)
            return successor
        }

        return null
    }

    override fun notifyFindValidNode4Get(id: Id): RingNode? {
        if (this.replicaDataStorage.containsEntries(nodeUrl, id)) {
            logger.debug("当前节点副本中存在数据,将返回此节点.节点:{},id:{}", nodeId, id)
            return this
        }

        if (shouldSelfProcess(id))
            return this

        val validSuccessor = findValidDirectSuccessor(id)
        return validSuccessor ?: references.findSuccessorWithinReferences4Get(id)
    }

    override fun notifyFindValidPreAndSuccessorsWithHint(potentialPredecessor: RingNode): PreAndSuccessors {
        withLock(notifyLock) {
            //首位是自己之前的前缀或者是刚通知的前缀
            val oldPredecessor = this.references.predecessor()
            val pre = if (oldPredecessor != null && oldPredecessor.isCurrentAlive) oldPredecessor else potentialPredecessor

            //追加剩下的后缀信息(仅添加有效的)
            val successorList = mutableListOf<ProxyNode>()
            this.references.successorList().successorList.stream()
                    .filter({ it.isCurrentAlive })
                    .forEach { successorList += it }

            //将新通知的前缀更新到引用表中(如果合适的话)
            if (potentialPredecessor.notifiable)
                this.references.addReferenceAsPredecessor(potentialPredecessor)

            return PreAndSuccessors(pre, successorList)
        }
    }

    @Throws(CommunicationException::class)
    override fun notifyFindValidPreAndSuccessors(): PreAndSuccessors {
        withLock(notifyLock) {
            val successorList = mutableListOf<ProxyNode>()
            this.references.successorList().successorList.stream()
                    .filter({ it.isCurrentAlive })
                    .forEach { successorList += it }

            return PreAndSuccessors(references.predecessor(), successorList)
        }
    }

    @Throws(CommunicationException::class)
    override fun notifyFindValidPreAndSuccessorsAndTransferDataWithHint(potentialPredecessor: RingNode): PreAndSucccessorsAndEntries {
        withLock(notifyLock) {
            //将原来存放在当前节点,现在应该存放在前缀节点中的数据找到,以便下一步作数据迁移时使用
            //这里的from to是从当前节点到前缀节点,即从逆时针范围内的数据,这就表示这些数据应该由前缀节点来处理,而从前缀到当前节点的数据本身就由当前节点处理
            val transferEntries = dataStorage.getEntriesInInterval<Any>(nodeUrl, nodeId, potentialPredecessor.nodeId)
            val preAndSuccessors = notifyFindValidPreAndSuccessorsWithHint(potentialPredecessor)

            return PreAndSucccessorsAndEntries(preAndSuccessors.pre, preAndSuccessors.successorList, transferEntries)
        }
    }

    @Throws(CommunicationException::class)
    override fun notifyChangeStoreToReplicas(potentialPredecessor: RingNode) {
        withLock(notifyLock) {
            //找到应该由前缀节点处理的数据(这里的数据由前缀处理了，因此自己就不再保存，而是转由副本存储)
            val changedEntries = dataStorage.getEntriesInInterval<Any>(nodeUrl, nodeId, potentialPredecessor.nodeId)
            dataStorage.removeAll(nodeUrl, changedEntries.keys)
            replicaDataStorage.addAll(nodeUrl, changedEntries)
        }
    }

    //---------------------------- 节点间数据操作 start ------------------------------//
    override fun <T> notifyPreInsertEntry(key: Key, toInsert: T, invokingNodeIdList: List<Id>) {
        val id = key.id

        val e = Entry(key, toInsert)
        dataStorage.add(nodeUrl, id, e)

        keyPointPreNotify(id, true, { it.notifyPreInsertEntry(key, toInsert, invokingNodeIdList + nodeId) })
    }

    override fun notifyPreDeleteEntry(entryToRemove: Key, invokingNodeIdList: List<Id>) {
        val id = entryToRemove.id

        dataStorage.remove(nodeUrl, id)

        //临界点处理
        keyPointPreNotify(id, true, { it.notifyPreDeleteEntry(entryToRemove, invokingNodeIdList + nodeId) })
    }

    override fun <T> notifyPreGetEntry(key: Key, invokingNodeIdList: List<Id>): T? {
        val id = key.id

        var result = dataStorage.getEntry<T>(nodeUrl, id)
        if (result != null)
            return result.value

        logger.debug("通知调用,从存储中没有找到数据,将从副本中返回数据.key:{}", key)
        result = replicaDataStorage.getEntry(nodeUrl, id)
        if (result != null)
            return result.value

        logger.debug("通知调用,从存储副本中没有找到数据.key:{}", key)

        //临界点处理
        return keyPointPreNotify(id, false, {
            logger.debug("通知调用,将从临界父类中查找数据.key:{}", key)
            return@keyPointPreNotify it.notifyPreGetEntry(key, invokingNodeIdList + nodeId)
        })
    }

    //---------------------------- 节点间数据操作 end ------------------------------//

    //---------------------------- 数据操作 start ------------------------------//

    /** 临界点判断,可能一个新的前缀刚好添加进来,并且此数据属于 oldPre < X < newPre < current的范围,进行相应的操作 */
    private fun <T> keyPointPreNotify(id: Id, asyncNotify: Boolean, notify: (ProxyNode) -> T?): T? {
        val predecessor = references.predecessor()
        //下面的判断，采用不在这范围内， 那么自然是新的前缀了，并且新的前缀比旧前缀理小一点(如果大的话，则仍然在此范围内，不满足条件)
        if (predecessor != null && !id.isInInterval(predecessor.nodeId, nodeId) && predecessor.isCurrentAlive) {
            return if (asyncNotify) {
                asyncExecutor.execute({ notify(predecessor) })
                null
            } else notify(predecessor)
        }

        return null
    }

    @Throws(CommunicationException::class)
    override fun <T> insertEntry(key: Key, toInsert: T) {
        val id = key.id

        val e = Entry(key, toInsert)
        dataStorage.add(nodeUrl, id, e)

        //将新添加数据作为副本通知后缀,以进行副本复制
        val copied = mutableMapOf(id to e)
        for (successor in references.successorList().successorList) {
            this.asyncExecutor.execute {
                try {
                    successor.notifyInsertReplicas(copied)
                } catch (e: CommunicationException) {
                    // do nothing
                }
            }
        }

        keyPointPreNotify(id, true, { it.notifyPreInsertEntry(key, toInsert, listOf(nodeId)) })
    }

    override fun <T> notifyInsertEntriees(insertEntries: Map<Id, Entry<T>>) {
        dataStorage.addAll(nodeUrl, insertEntries)
    }

    override fun <T> notifyInsertReplicas(insertReplicas: Map<Id, Entry<T>>) {
        replicaDataStorage.addAll(nodeUrl, insertReplicas)
    }

    @Throws(CommunicationException::class)
    override fun deleteEntry(entryToRemove: Key) {
        val id = entryToRemove.id

        dataStorage.remove(nodeUrl, id)

        //通知副本信息需要被删除
        val entriesToRemove = Sets.newHashSet(id)

        val successors = this.references.successorList().successorList
        for (successor in successors) {
            this.asyncExecutor.execute {
                try {
                    successor.notifyDeleteReplicas(nodeId, entriesToRemove)
                } catch (e: CommunicationException) {
                    logger.error("调用后缀节点删除错误失败。错误原因:{}", e.message, e)
                }
            }
        }

        //临界点处理
        keyPointPreNotify(id, true, { it.notifyPreDeleteEntry(entryToRemove, listOf(nodeId)) })
    }

    override fun notifyDeleteReplicas(sendingNode: Id, replicasToRemove: Set<Id>) {
        //指定了副本,则仅删除此副本即可
        if (!replicasToRemove.isEmpty()) {
            replicaDataStorage.removeAll(nodeUrl, replicasToRemove)
            return
        }

        logger.debug("准备移除范围内副本信息,数据范围从:{}到:{}", nodeId, sendingNode)
        logger.debug("移除范围内副本前当前存储数:{}", replicaDataStorage.getNumberOfStoredEntries(nodeUrl))
        val allReplicasToRemove = replicaDataStorage.getEntriesInInterval<Any>(nodeUrl, nodeId, sendingNode)
        replicaDataStorage.removeAll(nodeUrl, allReplicasToRemove.keys)

        logger.debug("移除范围内需要移除副本数:{},剩余存储数:{}", allReplicasToRemove.size, replicaDataStorage.getNumberOfStoredEntries(nodeUrl))
    }

    @Throws(CommunicationException::class)
    override fun <T> getEntry(key: Key): T? {
        val id = key.id

        var result = dataStorage.getEntry<T>(nodeUrl, id)
        if (result != null)
            return result.value

        logger.debug("从存储中没有找到数据,将从副本中返回数据.key:{}", key)
        result = replicaDataStorage.getEntry(nodeUrl, id)
        if (result != null)
            return result.value

        logger.debug("从存储副本中没有找到数据.key:{}", key)

        //临界点处理
        return keyPointPreNotify(id, false, {
            logger.debug("将从临界父类中查找数据.key:{}", key)
            return@keyPointPreNotify it.notifyPreGetEntry(key, listOf(nodeId))
        })
    }

    fun <T> getReplicaEntry(id: Id): Entry<T>? {
        return replicaDataStorage.getEntry(nodeUrl, id)
    }

    //---------------------------- 数据操作 end ------------------------------//

    override fun leaveNetwork(predecessor: RingNode) {
        val oldPre = this.references.predecessor()
        this.logger.info("收到原前缀离开网络的通知,当前节点:{},原前缀:{},新前缀:{}", this.nodeId, oldPre, predecessor)

        this.references.removeReference(oldPre!!)
    }

    private fun <T> addSelfData(map: Map<Id, Entry<T>>) {
        dataStorage.addAll(nodeUrl, map)
    }

    override fun leaveMayNotifyOthers() {
        //通知后缀节点前缀变化发生了变化
        try {
            val successor = references.successorList().directSuccessor
            val predecessor = references.predecessor()
            if (successor != null && predecessor != null) {
                successor.leaveNetwork(predecessor)
            }
        } catch (e: CommunicationException) {
            logger.debug("离开网络时通知后缀节点出错:" + e.message, e)
        }
    }

    @Suppress("UNCHECKED_CAST")
    @Throws(ServiceException::class)
    override fun processRefsInJoin(bootstrapNode: ProxyNode, newSuccessor: RingNode) {
        var mySuccessor = newSuccessor
        //准备处理前缀,以及迁移相应的数据
        var value: PreAndSucccessorsAndEntries
        try {
            value = mySuccessor.notifyFindValidPreAndSuccessorsAndTransferDataWithHint(this)
        } catch (e: CommunicationException) {
            throw ServiceException("加入网络时获取相应的引用信息及迁移数据时失败:" + e.message, e)
        }

        var predecessorSet = false
        while (!predecessorSet) {
            val pre = value.pre!!
            val successorList = value.successorList

            //之前网络中仅有1个节点,即引导节点,则此节点就是前缀节点,并且也是后缀节点
            if (successorList.isEmpty()) {
                logger.info("当前网络中仅有2个节点(包括自己),因此将后缀节点加为前缀节点:", mySuccessor)
                references.addReferenceAsPredecessor(mySuccessor)
                predecessorSet = true
            } else {
                //再次判断前后缀关系是否正确

                //当前节点确实是在后缀和获取数据前缀之间,则即认为是正确的前缀节点
                if (nodeId.isInInterval(pre.nodeId, mySuccessor.nodeId)) {
                    references.addReferenceAsPredecessor(pre)
                    predecessorSet = true
                } else {
                    //这里不知道为什么,可能是并发加入网络时出现冲突,因此整个后缀以及前缀处理都重新来,即之前的后缀在当前处理过程中找到了新的前缀,
                    //但此新的前缀反而比当前节点大了
                    //这里进入到else即意味着找到的前缀节点反而比当前id还大,因此继续从此前缀节点来查找引用信息
                    logger.info("之前找到错误的后缀节点,因此重新进行查找")

                    @Suppress("UnnecessaryVariable")
                    val adjustNewSuccessor = pre

                    if (adjustNewSuccessor is ProxyNode) {
                        this.addReference(adjustNewSuccessor)//此新的节点作为后缀
                    }

                    try {
                        value = adjustNewSuccessor.notifyFindValidPreAndSuccessorsAndTransferDataWithHint(this)
                        mySuccessor = adjustNewSuccessor
                    } catch (e: CommunicationException) {
                        throw ServiceException("加入网络时从新的后缀节点查找引用及迁移数据时失败:" + e.message, e)
                    }
                }
            }
        }

        // 将这些新的引用后缀信息,都加入到当前引用信息中,作为后缀来处理
        references.addReferences(value.successorList)

        //存储迁移过来的数据
        this.addSelfData(value.entries as Map<Id, Entry<Any>>)
        //准备将远程数据变更数据状态
        try {
            mySuccessor.notifyChangeStoreToReplicas(this)
        } catch (e: CommunicationException) {
            throw ServiceException("将后缀节点存储数据转为副本时失败:" + e.message, e)
        }
    }

    override fun <T> notifyGetEntriesInterval(fromId: Id, toId: Id): List<Entry<T>> {
        val vmap = dataStorage.getEntriesInInterval<T>(nodeUrl, fromId, toId)
        return vmap.values.toList()
    }

    //---------------------------- 报告及检测 start ------------------------------//

    override fun printEntries(): String {
        return dataStorage.toDebugString(nodeUrl)
    }

    override fun printFingerTable(): String {
        return references.fingerTable().toString()
    }

    override fun printSuccessorList(): String {
        return references.successorList().toString()
    }

    override fun printReferences(): String {
        return references.toString()
    }

    override fun printPredecessor(): String {
        return references.predecessor().toString()
    }

    override fun checkEntries() {
        val checkResult = AtomicBoolean(true)

        dataStorage.getEntries4Debug<Any>(nodeUrl).keys.forEach { id ->
            //正确在前缀和自己之间
            if (references.predecessor() != null && id.isInInterval(references.predecessor()!!.nodeId, id)) {
                return@forEach
            }

            //否则理论上的正确节点也是自己
            val successor = this.notifyFindValidNode(id) ?: this
            if (successor.nodeId == id) {
                return@forEach
            }

            //不满足相应条件,记error
            logger.error("检查数据时发现不一致数据.id值:{},数据值:{},当前节点:{},应存储节点:{}", id, dataStorage.getEntry<Any>(nodeUrl, id), id, successor.nodeId)
            logger.error("16进制表示,id值:{},当前节点:{},应存储节点:{}", id.toHexString(), id.toHexString(), successor.nodeId.toHexString())
            checkResult.set(false)
        }

        if (checkResult.get()) {
            logger.info("当前节点检查存储无误,没有超出此范围的数据.节点:{}", nodeId)
        }

    }

    //---------------------------- 报告及检测 start ------------------------------//
}