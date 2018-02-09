package io.iflym.dht.service.async

/**
 * 在异步future的基础之后,支持在执行完成之后,获取相应的执行返回结果
 *
 * @author flym
 */
interface ChordRetrievalFuture<out T> : ChordFuture {

    /**
     * 获取相应的执行的返回结果
     * 此方法调用会阻塞当前线程直到调用完成或者是有相应的异常发生
     *
     * @return 执行(如查询)的返回结果, 如果没有数据, 则返回空集合
     */
    val result: T?
}