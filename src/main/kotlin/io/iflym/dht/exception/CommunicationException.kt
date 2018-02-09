package io.iflym.dht.exception

/**
 * 通过节点间通信错误的异常信息
 *
 * @author flym
 */
class CommunicationException(message: String? = null, cause: Throwable? = null) : RuntimeException(message, cause)
