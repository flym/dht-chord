package io.iflym.dht.util

import com.iflym.core.util.StringUtils
import io.iflym.dht.exception.AssertException

import java.lang.reflect.InvocationTargetException

/** Created by flym on 6/23/2017.  */
object Asserts {
    /** 断言相应的判断应该为true  */
    fun assertTrue(result: Boolean, errorMessage: String, vararg params: Any) {
        if (!result) {
            throw AssertException(StringUtils.format(errorMessage, *params))
        }
    }

    fun assertTrue(result: Boolean, exClass: Class<out RuntimeException>, errorMessage: String, vararg params: Any) {
        if (!result) {
            val message = StringUtils.format(errorMessage, *params)
            try {
                throw exClass.getConstructor(String::class.java).newInstance(message)
            } catch (e: InstantiationException) {
                throw IllegalArgumentException(e.message, e)
            } catch (e: IllegalAccessException) {
                throw IllegalArgumentException(e.message, e)
            } catch (e: InvocationTargetException) {
                throw IllegalArgumentException(e.message, e)
            } catch (e: NoSuchMethodException) {
                throw IllegalArgumentException(e.message, e)
            }

        }
    }
}
