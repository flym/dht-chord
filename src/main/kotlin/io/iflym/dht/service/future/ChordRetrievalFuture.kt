package io.iflym.dht.service.future

import io.iflym.dht.model.Key
import io.iflym.dht.exception.ServiceException
import io.iflym.dht.service.chord.Chord
import io.iflym.dht.service.async.ChordRetrievalFuture
import java.util.*
import java.util.concurrent.Executor

/**
 * 用于实现在进行异步获取数据的future
 *
 * @author flym
 */
class ChordRetrievalFuture<out T>(
        /** 用于执行获取操作的主体chord  */
        private val chord: Chord,
        /** 待获取数据的key值  */
        private val key: Key
) : AbstractChordFuture(), ChordRetrievalFuture<T> {

    /** 最终异步执行之后获取的数据  */
    private var _result: T? = null

    private var _throwable: Throwable? = null

    override val throwable: Throwable?
        get() = _throwable

    override val result: T?
        get() {
            waitForBeingDone()

            //如果有异常,则直接throw异常
            val t = this.throwable
            if (t != null) {
                throw ServiceException(t.message, t)
            }
            return this._result
        }

    /** 返回用于实际执行获取任务的runnable对  */
    private val task: Runnable
        get() = Runnable {
            try {
                _result = (this.chord.get(this.key))
            } catch (t: Throwable) {
                _throwable = t
            }

            setIsDone()
        }

    companion object {
        /**
         * 工厂方法,用于构建相应的异步获取future对象
         *
         * @param exec 实际要执行获取操作的线程池对象
         */
        fun <T> of(exec: Executor, c: Chord, k: Key): io.iflym.dht.service.future.ChordRetrievalFuture<T> {
            Objects.requireNonNull(exec, "构建异步获取数据future时线程池对象不能为null")
            Objects.requireNonNull(exec, "构建异步获取数据future时相应的key值不能为null")

            val future = ChordRetrievalFuture<T>(c, k)
            exec.execute(future.task)
            return future
        }
    }
}