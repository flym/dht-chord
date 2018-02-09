package io.iflym.dht.util

import io.iflym.dht.concurrent.CaptionCallable
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit


object FutureUtils {
    private val logger = loggerFor<FutureUtils>()

    /** 可重试地执行,当执行失败时，允许在一定的间隔之后重新执行,直到成功为止 */
    fun <V> retriedInvoke(call: CaptionCallable<V>, executore: ScheduledExecutorService, retryDelay: Long, timeUnit: TimeUnit): CompletableFuture<V> {
        val future = CompletableFuture<V>()

        val invokeNum = 1

        var task: Runnable? = null
        task = Runnable {
            if (future.isCancelled)
                return@Runnable

            try {
                logger.debug("第{$invokeNum}次执行任务,${call.caption}")
                val v = call.call()
                future.complete(v)
            } catch (e: Throwable) {
                logger.info("执行任务失败，将重新执行.失败原因:{},重执行时间:{},{}", e.message, retryDelay, timeUnit)
                executore.schedule(task, retryDelay, timeUnit)
            }
        }

        executore.schedule(task, retryDelay, timeUnit)

        return future
    }
}