package io.iflym.dht.service.async

import io.iflym.dht.exception.ServiceException

/**
 * 用于描述在异步chord中通过异步操作立即返回的future对象,提供future语义
 * 调用者可通过future提供的多个方法来获取操作的处理状态,如是否完成.以及获取相应的结果,如出错的错误信息;也可以等待直到操作完成
 *
 * @author flym
 */
interface ChordFuture {

    /**
     * 如果在执行过程中出现了异常信息,则返回异常对象
     * 如果执行正常完成,则返回null
     * 调用者应该先判断isDone,以保证在操作完成之后再调用.如果在操作还没完成之前调用,则此方法仍返回null
     */
    val throwable: Throwable?

    /** 获取当前执行过程是否已执行完,则方法不会阻塞当前线程  */
    val isDone: Boolean

    /**
     * 阻塞当前线程直到相应的逻辑执行完
     * 执行完包括正常执行完,或者在过程中出现了异常信息
     * 调用者应该在执行完此方法之后,再使用其它的方法判断是正常结束还是异常退出
     */
    @Throws(ServiceException::class, InterruptedException::class)
    fun waitForBeingDone()
}