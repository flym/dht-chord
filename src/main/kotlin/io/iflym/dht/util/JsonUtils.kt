/** Created by flym at 11/18/2014  */
package io.iflym.dht.util

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.parser.ParserConfig
import com.alibaba.fastjson.parser.deserializer.ObjectDeserializer
import com.alibaba.fastjson.serializer.ObjectSerializer
import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.serializer.SerializerFeature
import com.google.common.collect.Lists
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.lang.reflect.Type

/**
 * json工具，用于格式化对象为json
 *
 * @author flym
 */
object JsonUtils {
    private val logger = LoggerFactory.getLogger(JsonUtils::class.java)
    private val jsonConfig = SerializeConfig()
    private val parseConfig = ParserConfig.getGlobalInstance()
    private var jsonFeatures: Array<SerializerFeature>? = null
    private var jsonFeaturesNotWriteClassName: Array<SerializerFeature>? = null

    init {
        var serializerFeatureList = Lists.newArrayList<SerializerFeature>()
        serializerFeatureList.add(SerializerFeature.WriteClassName)
        serializerFeatureList.add(SerializerFeature.SkipTransientField)
        serializerFeatureList.add(SerializerFeature.WriteDateUseDateFormat)
        serializerFeatureList.add(SerializerFeature.DisableCircularReferenceDetect)
        serializerFeatureList.add(SerializerFeature.PrettyFormat)

        jsonFeatures = serializerFeatureList.toTypedArray()

        serializerFeatureList = Lists.newArrayList()
        serializerFeatureList.add(SerializerFeature.SkipTransientField)
        serializerFeatureList.add(SerializerFeature.WriteDateUseDateFormat)
        serializerFeatureList.add(SerializerFeature.DisableCircularReferenceDetect)
        serializerFeatureList.add(SerializerFeature.PrettyFormat)
        jsonFeaturesNotWriteClassName = serializerFeatureList.toTypedArray()
    }

    init {
        //由于当前版本的asm生成有一定问题,因此不再使用asm生成(在1.1.43以后的版本,asmFactory已被重构)
        jsonConfig.isAsmEnable = false
        //与上同理
        parseConfig.isAsmEnable = false
    }

    /** 将对象输出为可反序列化的json字符串，以用于数据存储和传输  */
    fun <T> toJson(t: T): ByteArray {
        return toJson(t, true)
    }

    /**
     * 将对象输出为可反序列化的json字符串，以用于数据存储和传输
     *
     * @param writeClassName 是否输出类名
     */
    fun <T> toJson(t: T, writeClassName: Boolean): ByteArray {
        val features = if (writeClassName) jsonFeatures else jsonFeaturesNotWriteClassName

        return JSON.toJSONBytes(t, jsonConfig, *features!!)
    }

    fun addSerializer(type: Type, serializer: ObjectSerializer) {
        jsonConfig.put(type, serializer)
    }

    fun addDerializer(type: Type, deserializer: ObjectDeserializer) {
        parseConfig.putDeserializer(type, deserializer)
    }

    /** 将json字符串转换为指定类型的对象  */
    fun <T> parse(jsonBytes: ByteArray, clazz: Class<T>): T {
        try {
            return JSON.parseObject(jsonBytes, clazz)
        } catch (e: Exception) {
            throw RuntimeException(e.message, e)
        }
    }

    /** 将json字符串转换为指定类型的对象，如果不能转换，则返回指定的默认值  */
    fun <T> parse(jsonBytes: ByteArray, clazz: Class<T>, defaultValue: T): T {
        return try {
            JSON.parseObject(jsonBytes, clazz)
        } catch (e: RuntimeException) {
            logger.error(e.message, e)
            defaultValue
        }
    }

    /**
     * 将json字符串转换为指定的对象
     * 如果对象是按照指定的fastjson组织的，则转换为@type指定的对象，否则转换为JsonObject类型
     */
    @Suppress("UNCHECKED_CAST")
    fun <T> parse(jsonBytes: ByteArray): T {
        try {
            return JSON.parse(jsonBytes) as T
        } catch (e: Exception) {
            throw RuntimeException(e.message, e)
        }

    }
}
