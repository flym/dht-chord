package io.iflym.dht.net

/**
 * 对外服务状态变化监听器
 *
 * @author flym
 */
interface EndpointStateListener {

    /**
     * 通知监听器相应的对外服务端点状态已经发生了变化,新的状态值为参数状态值
     *
     * @param newState 现在新的状态值信息
     */
    fun notify(newState: Int)
}
