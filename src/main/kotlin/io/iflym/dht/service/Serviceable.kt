package io.iflym.dht.service

/**
 * 描述一个服务信息当前的运行状态
 * Created by tanhua(flym) on 7/25/2017.
 */
interface Serviceable {

    val serviceStatus: ServiceStatus

    /** 是否还没有初始化  */
    fun isUnInited() = serviceStatus === ServiceStatus.UN_INIT

    /** 是否正在运行中  */
    fun isRunning() = serviceStatus === ServiceStatus.RUNNING

    /** 是否已停止工作  */
    fun isStopped() = serviceStatus === ServiceStatus.STOPPED
}
