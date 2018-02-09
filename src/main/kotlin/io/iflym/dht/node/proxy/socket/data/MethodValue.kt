package io.iflym.dht.node.proxy.socket.data

/**
 * 描述了所有在node节点中开放的访问方法信息,在进行远程调用时,通过相应的定义来进行调用方法定位
 *
 *
 * 这里的定义与node接口中定义方法一致,如果node中新增接口方法,则这里也需要同步进行调整
 *
 *
 * @author flym
 */
enum class MethodValue {
    GET_NODE_ID,
    LEAVE_NETWORK,
    NOTIFY_CHANGE_STORE_TO_REPLICAS,


    INSERT_ENTRY,
    GET_ENTRY,
    DELETE_ENTRY,


    NOTIFY_GET_ENTRIES_INTERVAL,
    NOTIFY_FIND_VALID_NODE,
    NOTIFY_FIND_VALID_NODE_4_GET,
    NOTIFY_FIND_VALID_PRE_AND_SUCCESSORS,
    NOTIFY_FIND_VALID_PRE_AND_SUCCESSORS_WITH_HINT,
    NOTIFY_FIND_VALID_PRE_AND_SUCCESSORS_AND_TRANSFER_DATA_WITH_HINT,


    NOTIFY_INSERT_ENTRIES,
    NOTIFY_INSERT_REPLICAS,
    NOTIFY_DELETE_REPLICAS,
    NOTIFY_PRE_INSERT_ENTRY,
    NOTIFY_PRE_DELETE_ENTRY,
    NOTIFY_PRE_GET_ENTRY,

    //
    ;

}
