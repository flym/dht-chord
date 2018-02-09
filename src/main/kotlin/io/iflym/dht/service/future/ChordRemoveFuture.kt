package io.iflym.dht.service.future

import io.iflym.dht.model.Key
import io.iflym.dht.service.chord.Chord
import java.util.*
import java.util.concurrent.Executor

/**
 * 实现用于chord异步删除的future
 *
 * @author flym
 */
class ChordRemoveFuture(
        /** 执行删除逻辑的chord主体  */
        private val chord: Chord,
        /** 用于在删除时的key值  */
        private val key: Key
) : AbstractChordFuture() {

    override var throwable: Throwable? = null

    /** @return 返回实现用于处理逻辑的执行任务 */
    private val task: Runnable
        get() = Runnable {
            try {
                this.chord.delete(this.key)
            } catch (t: Throwable) {
                throwable = t
            }

            setIsDone()
        }

    companion object {

        /**
         * 工具方法,用于构建异步删除任务的future
         *
         * @param exec 执行异步任务的线程池
         */
        fun of(exec: Executor, c: Chord, k: Key): ChordRemoveFuture {
            Objects.requireNonNull(exec, "在构建异步数据删除future时线程池对象不能为null")
            Objects.requireNonNull<Any>(c, "在构建异步数据删除future时主体chord对象不能为null")
            Objects.requireNonNull<Any>(k, "在构建异步数据删除future时待删除的key值不能为null")

            val f = ChordRemoveFuture(c, k)
            exec.execute(f.task)
            return f
        }
    }
}
