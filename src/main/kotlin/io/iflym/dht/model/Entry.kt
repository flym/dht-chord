package io.iflym.dht.model

/**
 * 描述一个存储的数据信息
 *
 * @author flym
 */
data class Entry<T>(
        /** 相应的key值 */
        var key: Key,
        /** 实际存储值  */
        var value: T
)
