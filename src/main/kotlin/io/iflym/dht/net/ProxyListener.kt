package io.iflym.dht.net

import io.iflym.dht.node.ProxyNode

/**
 * 代理监听类,用于在代理类出现状态变化时作额外的操作
 * Created by flym on 7/25/2017.
 */
interface ProxyListener {
    fun serverDown(proxy: ProxyNode)
}
