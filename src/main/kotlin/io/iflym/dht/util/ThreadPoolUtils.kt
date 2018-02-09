package io.iflym.dht.util

import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.ThreadFactory
import java.util.concurrent.TimeUnit


object ThreadPoolUtils {
    /** 创建一个新的调度线程,允许核心线程退出,并且给出相应的退出时间值 */
    fun newScheduledPool(coreSize: Int, threadFactory: ThreadFactory, keepAliveTime: Long, keepAliveUnit: TimeUnit): ScheduledExecutorService {
        val value = ScheduledThreadPoolExecutor(coreSize, threadFactory)
        value.setKeepAliveTime(keepAliveTime, keepAliveUnit)
        value.allowCoreThreadTimeOut(true)

        return value
    }
}