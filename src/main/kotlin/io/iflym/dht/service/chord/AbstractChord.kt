package io.iflym.dht.service.chord

import com.iflym.core.util.ExceptionUtils
import io.iflym.dht.concurrent.CaptionCallable
import io.iflym.dht.exception.CommunicationException
import io.iflym.dht.exception.ServiceException
import io.iflym.dht.model.Id
import io.iflym.dht.model.Key
import io.iflym.dht.model.Url
import io.iflym.dht.node.ProxyNode
import io.iflym.dht.node.ReferencedNode
import io.iflym.dht.node.RingNode
import io.iflym.dht.service.ServiceStatus
import io.iflym.dht.service.async.AsynChord
import io.iflym.dht.service.async.ChordCallback
import io.iflym.dht.service.async.ChordFuture
import io.iflym.dht.service.chord.Chord.Companion.PROPERTY_PREFIX
import io.iflym.dht.service.future.ChordInsertFuture
import io.iflym.dht.service.future.ChordRemoveFuture
import io.iflym.dht.service.future.ChordRetrievalFuture
import io.iflym.dht.util.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit

/**
 * 用于实现一个主要的chord网络节点语义,即实现同步式chord网络交互,异步式网络交互以及数据存储的详细处理
 *
 * @author flym
 */
abstract class AbstractChord : Chord, AsynChord {

    /** 当前服务状态  */
    override var serviceStatus = ServiceStatus.UN_INIT
        protected set

    /** 当前日志记录器,初始时为一个未指定id的日志器 */
    protected var logger: Logger = loggerFor<AbstractChord>()

    /** 引用实现节点协议的node对象,在加入到网络中后,即会生成node对象  */
    protected var localNode: ReferencedNode? = null

    /** 用于执行异步处理的线程执行器  */
    var asyncExecutor: ScheduledExecutorService
        protected set

    /** 当前本地节点url  */
    override lateinit var url: Url
        protected set

    /** 当前本地chord的id值信息  */
    override lateinit var id: Id
        protected set

    init {
        this.asyncExecutor = ThreadPoolUtils.newScheduledPool(ASYNC_CALL_THREADS, ThreadFactoryUtils.build("chord-async-%d"), 1, TimeUnit.MINUTES)
    }

    private fun assertNodeNullWhenJoin() {
        Asserts.assertTrue(localNode == null, "当前节点已经加入到网络中, 不能再加入chord网络")
    }

    protected abstract fun initCreate()

    protected abstract fun postCreate()

    @Suppress("UNCHECKED_CAST")
    @Throws(ServiceException::class)
    override fun join(localURL: Url, localID: Id, vararg bootstrapURL: Url): CompletableFuture<Unit> {
        Objects.requireNonNull<Any>(localURL, "加入网络时指定的本地url不能为null")
        Objects.requireNonNull<Any>(localID, "加入网络时指定的本地id值不能为null")
        Objects.requireNonNull<Any>(bootstrapURL, "加入chord网络时引导url地址不能为null")
        assertNodeNullWhenJoin()

        this.url = localURL
        this.id = localID
        this.logger = LoggerFactory.getLogger(AccessChord::class.java.name + "." + this.id)

        initCreate()
        postCreate()

        serviceStatus = ServiceStatus.RUNNING

        return this.joinHelp(bootstrapURL as Array<Url>)
    }

    /**
     * 实现加入一个已有的网络的相应处理,包括设置引用表,后缀信息,数据迁移等
     * 在加入网络之前,相应的url地址和id值都已经设置好
     *
     * @param bootstrapUrlArray 相应的引导地址
     */
    @Throws(ServiceException::class)
    private fun joinHelp(bootstrapUrlArray: Array<Url>): CompletableFuture<Unit> {
        val call = CaptionCallable("节点加入集群任务:$id") {
            bootstrapUrlArray.forEach {
                if (it == localNode!!.nodeUrl)
                    return@forEach

                var hasError = true
                ExceptionUtils.doActionLogE(logger, {
                    //准备通过引导节点找到相应的前缀,后缀信息
                    val bootstrapNode: ProxyNode
                    try {
                        bootstrapNode = ProxyNode.of(this.url, it)
                    } catch (e: CommunicationException) {
                        logger.error("加入网络时连接引导节点时失败:" + e.message + ",引导:" + it, e)
                        return@doActionLogE
                    }

                    //引导节点可用,先加到引用表中
                    localNode!!.addReference(bootstrapNode)

                    // 通过引导节点可找到当前节点的直接后缀节点
                    val mySuccessor: RingNode
                    try {
                        mySuccessor = bootstrapNode.notifyFindValidNode(id) ?: bootstrapNode
                    } catch (e: CommunicationException) {
                        logger.error("加入网络时通过引导节点查找后缀节点时失败:" + e.message + ",引导:" + it, e)
                        return@doActionLogE
                    }

                    logger.info("加入网络,当前节点后缀节点:{},当前:{}", mySuccessor.nodeUrl, url)
                    if (mySuccessor is ProxyNode) {
                        localNode!!.addReference(mySuccessor)
                    }

                    localNode!!.processRefsInJoin(bootstrapNode, mySuccessor)

                    hasError = false
                })

                if (hasError)
                    return@CaptionCallable
            }
        }

        return FutureUtils.retriedInvoke(call, asyncExecutor, 3, TimeUnit.SECONDS)
    }

    override fun leave() {
        //还没有加入网络或创建起网络,直接跳过
        if (this.localNode == null) {
            return
        }

        localNode!!.leaveMayNotifyOthers()
        //网络节点关闭
        localNode!!.disconnect()
        //异步请求处理关闭
        asyncExecutor.shutdownNow()

        serviceStatus = ServiceStatus.STOPPED
    }

    override fun <T> insert(key: Key, obj: T) {
        val id = key.id

        var inserted = false
        while (!inserted) {
            //使用正确的节点来处理此请求
            val responsibleNode = localNode!!.findValidNode(id)
            try {
                responsibleNode.insertEntry(key, obj)
                inserted = true
            } catch (e: CommunicationException) {
                this.logger.debug("添加数据时发生通信错误异常:{},准备重试", e.message, e)
            }

        }
    }

    protected open fun <T> tryGetReplicaEntry(id: Id): T? {
        return null
    }

    override fun <T> get(key: Key): T? {
        Objects.requireNonNull<Any>(key, "获取数据时key不能为null")
        val id = key.id

        //先看一下,本地副本中有没有
        val replicaValue = tryGetReplicaEntry<T>(id)
        if (replicaValue != null)
            return replicaValue

        while (true) {
            val responsibleNode = localNode!!.findValidNode4Get(id)

            try {
                return responsibleNode.getEntry(key)
            } catch (e: CommunicationException) {
                this.logger.debug("获取数据时出现通信错误.异常:{},准备重试", e.message, e)
            }
        }
    }

    override fun delete(key: Key) {
        Objects.requireNonNull<Any>(key, "移除数据时key不能为null")

        val id = key.id

        var removed = false
        while (!removed) {
            val responsibleNode = localNode!!.findValidNode(id)
            try {
                responsibleNode.deleteEntry(key)
                removed = true
            } catch (e: CommunicationException) {
                this.logger.debug("在移除数据时出现通信网络错误.异常:{}", e.message, e)
            }

        }
    }

    override fun <T> retrieve(key: Key, callback: ChordCallback<T>) {
        val chord = this
        this.asyncExecutor.execute {
            var t: Throwable? = null
            var result: Set<T>? = null
            try {
                result = chord.get(key)
            } catch (th: Throwable) {
                t = th
            }

            callback.retrieved(key, result, t)
        }
    }

    override fun <T> insert(key: Key, entry: T, callback: ChordCallback<T>) {
        val chord = this
        this.asyncExecutor.execute {
            var t: Throwable? = null
            try {
                chord.insert(key, entry)
            } catch (th: Throwable) {
                t = th
            }

            callback.inserted(key, entry, t)
        }
    }

    override fun <T> remove(key: Key, entry: T, callback: ChordCallback<T>) {
        val chord = this
        this.asyncExecutor.execute {
            var t: Throwable? = null
            try {
                chord.delete(key)
            } catch (th: Throwable) {
                t = th
            }

            callback.removed(key, entry, t)
        }
    }

    override fun <T> retrieveAsync(key: Key): io.iflym.dht.service.async.ChordRetrievalFuture<T> {
        return ChordRetrievalFuture.of(this.asyncExecutor, this, key)
    }

    override fun <T> insertAsync(key: Key, entry: T): ChordFuture {
        return ChordInsertFuture.of(this.asyncExecutor, this, key, entry)
    }

    override fun <T> removeAsync(key: Key, entry: T): ChordFuture {
        return ChordRemoveFuture.of(this.asyncExecutor, this, key)
    }

    companion object {

        /**
         * 用于执行异步请求及处理的固定线程数
         * 如执行[insert] 这样的处理时的线程池线程数
         */
        protected val ASYNC_CALL_THREADS = Integer.parseInt(System.getProperty(PROPERTY_PREFIX + ".AsyncThread.no"))

        /** 用于维护其后缀节点的周期性任务的起始延迟时间,单位:秒  */
        val STABILIZE_TASK_START = Integer.parseInt(System.getProperty(PROPERTY_PREFIX + ".StabilizeTask.start"))

        /** 用于维护其后缀节点的周期性任务的周期性间隔时间,单位:秒  */
        val STABILIZE_TASK_INTERVAL = Integer.parseInt(System.getProperty(PROPERTY_PREFIX + ".StabilizeTask.interval"))

        /** 用于维护finger索引的周期性任务的起始延迟时间,单位:秒  */
        val FIX_FINGER_TASK_START = Integer.parseInt(System.getProperty(PROPERTY_PREFIX + ".FixFingerTask.start"))

        /** 用于维护finger索引的的周期性任务的周期性执行时间,单位:秒  */
        val FIX_FINGER_TASK_INTERVAL = Integer.parseInt(System.getProperty(PROPERTY_PREFIX + ".FixFingerTask.interval"))

        /** 用于周期性检查前缀节点的任务的起始任务延迟时间,单位:秒  */
        val CHECK_PREDECESSOR_TASK_START = Integer.parseInt(System.getProperty(PROPERTY_PREFIX + ".CheckPredecessorTask.start"))

        /** 用于周期性检查前缀节点的任务的周期性执行时间,单位:秒  */
        val CHECK_PREDECESSOR_TASK_INTERVAL = Integer.parseInt(System.getProperty(PROPERTY_PREFIX + ".CheckPredecessorTask.interval"))
    }

}