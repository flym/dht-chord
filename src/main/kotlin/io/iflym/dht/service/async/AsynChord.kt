package io.iflym.dht.service.async

import io.iflym.dht.model.Key
import io.iflym.dht.service.chord.Chord

/**
 *
 *
 * 描述一个支持异步操作的chord分布式网络
 *
 * 方法 insert delete, retrieve均支持传入一个额外的[ChordCallback] 以支持相应的异步回调,即操作会马上返回,但实际数据返回之后再
 * 回调相应的接口
 * 而方法 insertAsync,removeAsync,retriveAsync则通过返回一个future以支持调用端进行后续处理,调用者可以通过future来考虑下一步动作,也可以
 * 等待相应的操作完成
 *
 *
 * @author flym
 */
interface AsynChord : Chord {
    /**
     * 异步地获取指定key下存储的数据信息,当数据可用时通过相应的数据回调.
     * 此方法将立即返回,而不需要等待处理返回.并通过@[ChordCallback.retrieved] 进行回调
     *
     * @param key      待获取存储数据相对应的key值
     * @param callback 回调函数,相应的结果会放入回调参数中
     */
    fun <T> retrieve(key: Key, callback: ChordCallback<T>)

    /**
     * 异步地往chord网络中存储指定key和相应值的存储数据,待完成之后通知相应的回调函数
     * 此方法将立即返回,相应的结果会放入回调参数中.并通过@[ChordCallback.inserted]进行回调
     *
     * @param key      相应数据的key值
     * @param entry    实际存储的数据信息
     * @param callback 回调函数,相应的结果会放入回调参数中
     */
    fun <T> insert(key: Key, entry: T, callback: ChordCallback<T>)

    /**
     * 异步地从chord网络中移除指定key及相应的存储数据,待完成之后通知相应的回调函数
     * 此方法将立即返回,而不需要等待处理返回.并通过@[ChordCallback.removed]进行回调
     *
     * @param key      相应数据的key值
     * @param entry    要删除的数据值
     * @param callback 回调函数,相应的处理结果会放入回调参数中
     */
    fun <T> remove(key: Key, entry: T, callback: ChordCallback<T>)

    /**
     * 异步获取数据,并立即返回一个future,调用方可通过future进行下一步动作,如果数据可用,可通过@[ChordRetrievalFuture.result]
     * 获取相应的结果
     *
     * @param key 要获取的数据的key值
     */
    fun <T> retrieveAsync(key: Key): ChordRetrievalFuture<T>

    /**
     * 异步地存储数据,并立即返回一个future,调用方可通过future进行下一步动作.
     *
     * @param key   待存储数据的key值
     * @param entry 实际的存储数据
     */
    fun <T> insertAsync(key: Key, entry: T): ChordFuture

    /**
     * 异步地移除数据,并立即返回一个future,调用方可通过future进行下一步动作.
     *
     * @param key   要删除数据的key值
     * @param entry 实际要删除的数据
     */
    fun <T> removeAsync(key: Key, entry: T): ChordFuture
}
