package io.iflym.dht.node

/**
 * 提供一个报告chord节点内部信息的能力,报告的信息包括它的存储信息,访问路由表,后缀表,前缀信息等
 *
 * @author flym
 */
interface Reportable {

    /** 打印节点内部存储的所有存储数据  */
    fun printEntries(): String

    /** 打印节点内部存储的的访问路由表信息  */
    fun printFingerTable(): String

    /** 打印节点的后缀节点信息  */
    fun printSuccessorList(): String

    /** 打印节点的引用信息(包括前缀,后缀,路由表信息)  */
    fun printReferences(): String

    /** 打印节点的前缀节点信息  */
    fun printPredecessor(): String

    /** 检查存储的数据是否都应该为自己存储  */
    fun checkEntries()
}
