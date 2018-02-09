package io.iflym.dht.model

import com.google.common.collect.ImmutableList

/**
 * 描述一个节点的访问地址信息
 *
 *
 * 地址信息一旦确定,将不再可变,即此对象是不可变的
 *
 * @author flym
 */
class Url(
        /** 协议部分  */
        val protocol: String,
        /** 主机部分  */
        val host: String,
        /** 端口  */
        val port: Int,
        /** 后续路径(端口后面的部分)  */
        val path: String
) {
    /** 惟一字符串 */
    private val uniqueStr: String by lazy { generateUniqueStr() }

    private fun generateUniqueStr(): String = "$protocol/$host/$port/$path"

    fun uniqueString() = uniqueStr

    /** 地址全名称  */
    private val urlString by lazy { generateUrlString() }

    private fun generateUrlString(): String {
        val builder = StringBuilder()
        builder.append(this.protocol)
        builder.append(DCOLON_SLASHES)
        builder.append(this.host)
        builder.append(DCOLON)
        builder.append(this.port)
        builder.append(SLASH)
        builder.append(this.path)
        return builder.toString().toLowerCase()
    }

    override fun toString() = urlString

    companion object {
        /** 协议的默认端口信息  */
        private val DEFAULT_PORTS = intArrayOf(4242, -1)

        private const val SOCKET_PROTOCOL = 0
        const val PROTOCOL_SOCKET = "ocsocket"

        /** 当前已知支持的地址协议信息  */
        private val KNOWN_PROTOCOLS: List<String> = ImmutableList.of(PROTOCOL_SOCKET)

        private const val DCOLON = ":"
        private const val SLASH = "/"
        private const val DCOLON_SLASHES = DCOLON + SLASH + SLASH

        fun create(str: String): Url {
            var url = str
            //协议部分
            val indexOfColonAndTwoSlashes = url.indexOf(DCOLON_SLASHES)
            if (indexOfColonAndTwoSlashes < 0) {
                throw IllegalArgumentException("地址没有协议信息:" + url)
            }
            val protocol = url.substring(0, indexOfColonAndTwoSlashes)
            // 协议检查
            val protocolIsKnown = KNOWN_PROTOCOLS.contains(protocol)
            if (!protocolIsKnown) {
                throw IllegalArgumentException("不支持的协议信息:" + protocol)
            }

            val host: String
            val port: Int

            //跳过://部分
            url = url.substring(indexOfColonAndTwoSlashes + DCOLON_SLASHES.length)
            var endOfHost = url.indexOf(DCOLON)
            if (endOfHost >= 0) {
                host = url.substring(0, endOfHost)
                url = url.substring(endOfHost + 1)

                val endOfPort = url.indexOf(SLASH)
                //端口后面需要接路径信息
                if (endOfPort < 0) {
                    throw IllegalArgumentException("端口后必须接路径信息:" + url)
                }

                val tmpPort = Integer.parseInt(url.substring(0, endOfPort))
                //端口范围要求
                if (tmpPort <= 0 || tmpPort >= 65536) {
                    throw IllegalArgumentException("不是有效地址,端口必须在0到65536之间:" + url)
                }
                port = tmpPort

                //路径部分
                url = url.substring(endOfPort + 1)
            } else {
                endOfHost = url.indexOf(SLASH)
                if (endOfHost < 0) {
                    throw IllegalArgumentException("Not a valid URL")
                }
                host = url.substring(0, endOfHost)

                //路径部分
                url = url.substring(endOfHost + 1)

                //产生默认端口
                if (protocol == PROTOCOL_SOCKET) {
                    port = DEFAULT_PORTS[SOCKET_PROTOCOL]
                } else {
                    port = -1
                }
            }
            val path = url

            return Url(protocol, host, port, path)
        }
    }
}