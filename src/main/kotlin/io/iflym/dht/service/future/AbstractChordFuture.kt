package io.iflym.dht.service.future

import io.iflym.dht.exception.ServiceException
import io.iflym.dht.service.async.ChordFuture

/**
 * 描述一个主要的chord异步future实现的主体部分
 *
 * @author flym
 */
abstract class AbstractChordFuture : ChordFuture {
    private val obj = Object()

    /** 当前任务是否已完成的标识  */
    private var _isDone = false

    /** 设置当前逻辑已完成  */
    @Synchronized
    fun setIsDone() {
        this._isDone = true
        synchronized(obj) {
            obj.notifyAll()
        }
    }

    override val isDone: Boolean
        get() = if (this.throwable != null) throw ServiceException(this.throwable!!.message, this.throwable) else this._isDone

    @Throws(ServiceException::class, InterruptedException::class)
    override fun waitForBeingDone() {
        synchronized(obj) {
            while (!this.isDone) {
                obj.wait()
            }
        }
    }
}
