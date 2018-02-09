package io.iflym.dht.service.async

import io.iflym.dht.model.Key

/**
 * chord异步网络节点操作的处理回调
 *
 *
 * 对应于主要数据的3个操作,get,insert,remove均有相对应的3个回调操作.
 * 在回调函数中,如果没有异步发生,则相应的throwable参数为null
 *
 * @author flym
 */
interface ChordCallback<in T> {

    /**
     * 处理[AsynChord.get]的数据回调
     *
     * @param key     获取数据时传递的key值
     * @param entries 获取回来的的数据值,如果没有数据与key对象,则返回空的集合
     * @param t       如果在操作处理中出现了异常信息,则传递相应的异常对象
     */
    fun retrieved(key: Key, entries: Set<T>?, t: Throwable?)

    /**
     * 处理[AsynChord.insert]的数据回调
     *
     * @param key   待插入数据的key值
     * @param entry 实际要存储的数据值
     * @param t     如果在操作过程中出现了异常信息,则传递相应的异常对象
     */
    fun inserted(key: Key, entry: T?, t: Throwable?)

    /**
     * 处理[AsynChord.delete]的数据回调
     *
     * @param key   要删除数据的key值
     * @param entry 实际要删除的数据(即调用时传递的删除数据)
     * @param t     如果在操作过程中出现了异常信息,则传递相应的异常对象
     */
    fun removed(key: Key, entry: T?, t: Throwable?)

}
