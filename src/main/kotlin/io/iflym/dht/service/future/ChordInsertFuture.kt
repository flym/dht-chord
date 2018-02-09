package io.iflym.dht.service.future

import io.iflym.dht.model.Key
import io.iflym.dht.service.chord.Chord
import java.util.*
import java.util.concurrent.Executor

/**
 * 实现chord异步插入的future
 *
 * @author flym
 */
class ChordInsertFuture<T>(
        /** 反向引用执行相应操作的主体节点  */
        private val chord: Chord,

        /** 当前执行插入时的key  */
        private val key: Key,

        /** 具体要插入的数据  */
        private val entry: T

) : AbstractChordFuture() {

    override var throwable: Throwable? = null

    /** @return 返回实现用于执行任务的runnable对象*/
    private val task: Runnable
        get() = Runnable {
            try {
                this.chord.insert(this.key, this.entry)
            } catch (t: Throwable) {
                throwable = t
            }

            setIsDone()
        }

    companion object {

        /**
         * 工具方法,用于构建一个future对象
         *
         * @param exec 用于执行异步任务的线程池
         */
        fun <T> of(exec: Executor, c: Chord, k: Key, entry: T): ChordInsertFuture<T> {
            Objects.requireNonNull(exec, "构建异步插入future时线程池对象不能为null")
            Objects.requireNonNull<Any>(c, "构建异步插入future时主体chord对象不能为null")
            Objects.requireNonNull<Any>(k, "构建异步插入future时数据key不能为null")
            Objects.requireNonNull(entry, "构建异步插入future时存储数据不能为null")

            val f = ChordInsertFuture(c, k, entry)
            exec.execute(f.task)
            return f
        }
    }
}
