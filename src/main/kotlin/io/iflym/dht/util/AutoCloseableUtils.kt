package io.iflym.dht.util

/** Created by flym on 7/17/2017.  */
object AutoCloseableUtils {
    fun closeQuietly(closeable: AutoCloseable) {
        try {
            closeable.close()
        } catch (e: Exception) {
            //nothing to do
        }

    }
}
