package io.iflym.dht.service.chord

import io.iflym.dht.exception.ServiceException
import io.iflym.dht.model.Id
import io.iflym.dht.model.Key
import io.iflym.dht.model.Url
import io.iflym.dht.node.Reportable
import io.iflym.dht.service.Serviceable
import java.util.concurrent.CompletableFuture

/**
 * 描述了一个chord网络对于用户程序来说,其应该提供一个什么样的能力. 即作为一个chord程序,它应该基本提供什么样的操作,以支持网络结构建设(如创建网络,
 * 加入网络,离开网络),以及数据存储能力(添加,获取,删除)
 *
 *
 * 对一个chord网络节点来说,url地址和id标识是其内部的2个必要属性,并且仅能设置一次.在构建过程中,如果出现了覆盖或者修改,则报错.
 *
 * @author flym
 */
interface Chord : Serviceable {

    /** 获取本地节点的地址  */
    val url: Url

    /** 获取本地节点的id标识信息  */
    val id: Id

    //---------------------------- 集群操作类 start ------------------------------//

    /**
     * 加入一个已经存在的chord网络,并且通过指定地址作为引导入口处理.
     * 当前节点使用指定的本地url和id标识信息,要求之前没有设置过相应信息
     *
     * @param localURL     本地url地址
     * @param localID      本地节点id标识
     * @param bootstrapURL chord网络中引导节点url地址
     */
    @Throws(ServiceException::class)
    fun join(localURL: Url, localID: Id, vararg bootstrapURL: Url): CompletableFuture<Unit>

    /** 从chord网络中退出  */
    @Throws(ServiceException::class)
    fun leave()

    //---------------------------- 集群操作类 end ------------------------------//


    //---------------------------- 数据访问类 start ------------------------------//

    /**
     * 往chord网络中添加一个新的数据,使用指定的key和数据
     * 数据并不是直接加入到本地chord节点,而是按照指定的规则放到网络中,即在网络中查找合适的节点并进行存储
     * 如果多个相同数据均具有相同的key,则聚合在一起
     *
     * @param key    存储数据的key信息
     * @param obj 具体的存储数据
     */
    @Throws(ServiceException::class)
    fun <T> insert(key: Key, obj: T)

    /**
     * 从chord网络中找到指定key所对应的的存储数据信息
     *
     * @param key 存储数据所对应的key值
     */
    @Throws(ServiceException::class)
    fun <T> get(key: Key): T?

    /**
     * 从chord网络中删除之前存储的数据,此数据具有指定的key值和存储值
     *
     * @param key    存储数据相应的key值
     */
    @Throws(ServiceException::class)
    fun delete(key: Key)

    /**
     *  返回下层的报告节点
     *  主要目的用于报告相应行为信息以及检查数据
     * */
    fun reportedNode(): Reportable?

    //---------------------------- 数据访问类 start ------------------------------//

    companion object {
        val PROPERTY_PREFIX = Chord::class.java.name!!
    }
}