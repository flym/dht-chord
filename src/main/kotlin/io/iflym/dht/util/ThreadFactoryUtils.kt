package io.iflym.dht.util

import com.google.common.util.concurrent.ThreadFactoryBuilder

import java.util.concurrent.ThreadFactory

/**
 * 线程工厂工具类,更简单地创建起工厂实例
 * Created by tanhua(flym) on 7/21/2017.
 */
object ThreadFactoryUtils {
    fun build(nameFormat: String): ThreadFactory {
        return ThreadFactoryBuilder().setNameFormat(nameFormat).build()
    }

    fun build(nameFormat: String, backingThreadFactory: ThreadFactory): ThreadFactory {
        return ThreadFactoryBuilder().setNameFormat(nameFormat).setThreadFactory(backingThreadFactory).build()
    }
}
