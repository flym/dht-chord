package io.iflym.dht.concurrent

import java.util.concurrent.Callable


/** 描述有标题的任务 */
class CaptionCallable<V>(override val caption: String, private val callable: () -> V) : Callable<V>, Captioned {
    override fun call(): V = callable()
}