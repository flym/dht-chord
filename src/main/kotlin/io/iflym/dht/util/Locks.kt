package io.iflym.dht.util

import java.util.concurrent.locks.Lock


/** 使用锁进行相应事务处理,此方法封闭了锁的获取和业务处理,即外部不再需要调用lock方法 */
inline fun <R> withLock(lock: Lock, block: () -> R): R {
    lock.lock()
    try {
        return block()
    } finally {
        lock.unlock()
    }
}