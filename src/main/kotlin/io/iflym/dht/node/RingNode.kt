package io.iflym.dht.node

import io.iflym.dht.data.PreAndSucccessorsAndEntries
import io.iflym.dht.data.PreAndSuccessors
import io.iflym.dht.model.Entry
import io.iflym.dht.model.Id
import io.iflym.dht.model.Key
import io.iflym.dht.model.Url
import io.iflym.dht.service.Serviceable


/**
 *  描述一个存在于环上的一个节点
 *  存在于环上的节点并不一定要存储数据，仅用于表示其在物理上表示特定的点，并且每个点都可以进行数据的相应操作，以及与其它的节点进行交互
 * 同时，节点间的数据复制等均通过节点来完成。
 * 需要注意的是，对于节点间的数据通知操作，将限制通知的次数，以避免无限次通知操作。
 * 如 在数据获取过程中，A->B->C->D->A(其中B为A父级，C为B父级,D为C父级,A为D父级), 在潜在的环中，如果向父级通知操作，将不可避免地导致无限循环的问题,
 * 为解决此问题，在通知操作中，将传递自己及之前调用节点的信息，当发现环级时，直接返回数据。
 * 同时，增加相应的调用深度判断(深度信息由调用节点列表得出)，如果超过指定深度，则同样直接返回数据
 *
 * 节点间数据复制将不具有传递性，即不会传递给后缀节点等,这些传递操作由原始节点已完成相应处理
 *  @author flym
 *
 * */
interface RingNode : AutoCloseable, Serviceable {
    /** 节点id信息  */
    val nodeId: Id

    /** 节点地址信息(即实际存储数据的节点地址)  */
    val nodeUrl: Url

    //---------------------------- 节点类相应操作 start ------------------------------//

    /** 找到应该处理此数据的节点，如果不能找到，则返回null */
    fun notifyFindValidNode(id: Id): RingNode?

    /** 为了获取数据的目的找到能够处理相应key的节点信息 ,如果不能找到，则返回null */
    fun notifyFindValidNode4Get(id: Id): RingNode?

    /** 获取在两者之间的数据信息 fromId < x <= toId */
    fun <T> notifyGetEntriesInterval(fromId: Id, toId: Id): List<Entry<T>>

    /**
     * 找到当前节点的前缀及后缀节点列表,以用于其它节点查看自己的状态信息
     * 仅返回当前网络中可访问的
     */
    fun notifyFindValidPreAndSuccessors(): PreAndSuccessors

    /**
     * 请求当前节点的前缀节点以及后缀节点信息,以用于节点间通信判断.
     * 如一个节点认为它是当前节点的父节点,通过获取当前节点之前存储的数据信息,以进行进一步判断和处理,保证整个前后缀是OK的.
     * 只返回当前在网络访问当前可访问的节点
     *
     * @param potentialPredecessor 可能的前缀节点,即此对象可能是当前的前缀节点
     * @return 当前节点的前缀节点(并不一定与potentialPre相同)以及当前节点的后缀节点列表. 前缀节点为 0位置,后缀依次靠后
     */
    fun notifyFindValidPreAndSuccessorsWithHint(potentialPredecessor: RingNode): PreAndSuccessors

    /**
     * 请求当前节点的前缀节点,后缀节点以及当前节点存储的数据信息
     * 请求回来的存储数据被认为当前应该被相应的potentialPre接管(最终可能将相应的数据转移到potentialPre节点上)
     *
     * @param potentialPredecessor 可能的前缀节点
     */
    fun notifyFindValidPreAndSuccessorsAndTransferDataWithHint(potentialPredecessor: RingNode): PreAndSucccessorsAndEntries

    /**
     * 将当前节点与前缀节点之间的存储数据转为副本
     * 此场景在于相应的前缀节点已经做完迁移数据了,因此原来当前节点存储的数据将转为副本数据进行存储
     */
    fun notifyChangeStoreToReplicas(potentialPredecessor: RingNode)

    //---------------------------- 节点类相应操作 end ------------------------------//


    //---------------------------- 节点间数据相应操作 start ------------------------------//

    /** 通知此节点(作为调用节点的前缀)应该存储此数据信息  */
    fun <T> notifyPreInsertEntry(key: Key, toInsert: T, invokingNodeIdList: List<Id>)

    /**
     * 通知此节点(作为调用节点的前缀)删除相应的数据信息
     *
     * @param entryToRemove 实际要删除的数据
     */
    fun notifyPreDeleteEntry(entryToRemove: Key, invokingNodeIdList: List<Id>)

    /*** 获取此节点(作为调用节点的前缀)获取指定key下存储的数据信息 */
    fun <T> notifyPreGetEntry(key: Key, invokingNodeIdList: List<Id>): T?

    fun <T> notifyInsertEntriees(insertEntries: Map<Id, Entry<T>>)

    /**
     * 存储上游节点存储之后, 当前节点认为需要进行复制存储的数据
     * 实际实现时,可以忽略此请求. 即当前节点接收到一个副本数据存储请求.
     *
     * @param insertReplicas 要进行复制的副本数据
     */
    fun <T> notifyInsertReplicas(insertReplicas: Map<Id, Entry<T>>)

    /**
     * 移除指定前缀节点下原先因为副本处理而存储,的本地副本数据
     * 即原先进行了副本存储,现在需要移除这些副本
     *
     * @param sendingNode      前缀节点id
     * @param replicasToRemove 要移除的数据集. 如果为empty,则所有小于前缀节点id的数据均删除.即表示移除所有前缀副本
     */
    fun notifyDeleteReplicas(sendingNode: Id, replicasToRemove: Set<Id>)

    //---------------------------- 节点间数据相应操作 end ------------------------------//

    //---------------------------- 自己提供的节点操作 start ------------------------------//

    /**
     * 找到应该处理相应数据key的节点信息
     * 每个节点只处理一部分数据,但每个节点都可以根据发送过来的请求从自己以及自己的后缀中定位到实际处理数据的节点
     * 只返回当前在网络访问中当前可访问的节点
     * 当 不能找到节点时，最终返回自己
     *
     * @param id 表示数据请求的id信息
     */
    fun findValidNode(id: Id): RingNode = notifyFindValidNode(id) ?: this

    /**
     * 为了获取数据的目的找到能够处理相应key的节点信息
     * 主要的目的在于利用起相应的副本策略,并不总是到实际存储的节点上去找数据
     * 只返回当前在网络访问中当前可访问的节点
     * 当不能找到数据时，最终返回自己
     */
    fun findValidNode4Get(id: Id): RingNode = notifyFindValidNode4Get(id) ?: this


    //---------------------------- 自己提供的节点操作 end ------------------------------//


    //---------------------------- 数据的相应操作 start ------------------------------//

    /** 存储相应的数据信息  */
    fun <T> insertEntry(key: Key, toInsert: T)

    /**
     * 删除相应的数据信息
     *
     * @param entryToRemove 实际要删除的数据
     */
    fun deleteEntry(entryToRemove: Key)

    /**
     * 获取指定key下存储的数据信息
     */
    fun <T> getEntry(key: Key): T?

    //---------------------------- 数据的相应操作 end ------------------------------//

    //---------------------------- 生命周期类操作 start ------------------------------//

    /** 必要的初始化操作 */
    fun init()

    /** 开始提供服务 */
    fun start()

    /**
     * 通知当前节点,它的前缀节点已经从网络中移除.以方便当前节点进行一系列后续处理.
     *
     * @param predecessor 新的参考前缀节点,即当前节点的前缀节点之前的前缀节点移除之后,当前节点的新前缀节点
     */
    fun leaveNetwork(predecessor: RingNode)

    /**
     * 关掉当前节点对外的所有连接,意味着当前节点准备从网络中移除
     * 如果是代理,则并不表示真实节点被断开,仅表示代理连接断开.(代理可以重建,不会触发实际节点断开网络)
     */
    fun disconnect()

    override fun close() = disconnect()

    //---------------------------- 生命周期类操作 start ------------------------------//

    //---------------------------- 支持特殊的类型信息 start ------------------------------//

    /** 此节点是否可用于节点间访问的 */
    val notifiable: Boolean

    //---------------------------- 支持特殊的类型信息 end ------------------------------//
}