package io.iflym.dht.exception

/**
 * 描述一个在当前服务层出错时发生的异常
 * @author flym
 *
 **/
class ServiceException constructor(message: String? = null, e: Throwable? = null) : RuntimeException(message, e)
