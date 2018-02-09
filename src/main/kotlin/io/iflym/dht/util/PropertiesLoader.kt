package io.iflym.dht.util

import com.google.common.base.Strings
import com.iflym.core.util.StringUtils
import java.io.IOException

/**
 * 简单的属性加载器,用于加载整个chord网络所需要的一些配置信息
 *
 * @author flym
 */
object PropertiesLoader {

    /** 标记_配置信息是否已加载  */
    private var loaded = false

    /** 属性配置项_可额外指定配置文件的属性名  */
    private const val PROPERTY_WHERE_TO_FIND_PROPERTY_FILE = "chord.properties.getFile"

    /** 默认配置属性文件  */
    private const val STANDARD_PROPERTY_FILE = "chord.properties"

    /** 加载配置信息,从扩展配置属性名(如果提供)或者是默认的位置进行属性加载  */
    fun loadPropertyFile() {
        if (loaded)
            throw IllegalStateException("属性信息已经加载过")

        loaded = true

        var file = STANDARD_PROPERTY_FILE

        val extPropertyFile = System.getProperty(PROPERTY_WHERE_TO_FIND_PROPERTY_FILE)
        if (!Strings.isNullOrEmpty(extPropertyFile)) {
            file = extPropertyFile
        }

        try {
            val props = System.getProperties()
            props.load(ClassLoader.getSystemResourceAsStream(file))
            System.setProperties(props)
        } catch (e: IOException) {
            val message = StringUtils.format("加载属性信息失败,属性文件名:{},失败原因:{}", file, e.message)
            throw RuntimeException(message, e)
        } catch (e: NullPointerException) {
            val message = StringUtils.format("加载属性信息失败,属性文件名:{},失败原因:{}", file, e.message)
            throw RuntimeException(message, e)
        }

    }
}
