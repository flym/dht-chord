package io.iflym.dht

import io.iflym.dht.model.Id
import io.iflym.dht.model.Key
import io.iflym.dht.model.Url
import io.iflym.dht.service.chord.AccessChord
import io.iflym.dht.service.chord.PersistentChord
import io.iflym.dht.util.PropertiesLoader
import org.slf4j.LoggerFactory
import java.util.concurrent.ThreadLocalRandom

object Test {
    val log = LoggerFactory.getLogger(Test.javaClass)
    private val URL1 = "ocsocket://localhost:1234/"
    private val URL2 = "ocsocket://localhost:1235/"
    private val URL3 = "ocsocket://localhost:1236/"

    private var url1 = Url.create(URL1)
    private var url2 = Url.create(URL2)
    private var url3 = Url.create(URL3)

    private var id1 = Id.of("a")
    private var id2 = Id.of("b")
    private var id3 = Id.of("c")


    private var urlx = Url.create("ocsocket://localhost:8080/")
    private var idx = Id.of("x")

    @Throws(Exception::class)
    private fun testRetrive(vararg urls: Url) {
        var found = true

        for (url in urls) {
            val accessChord = AccessChord()
            accessChord.join(urlx, idx, url)
            for (i in 10..29) {
                val strKey = "x" + i
                val valueSet = accessChord.get<Any>(Key.of(strKey))
                if (valueSet != null) {
                    log.error("在查找key并没有找到相应的值.key:{}", strKey)
                    found = false
                }
            }
            accessChord.leave()
        }

        if (found)
            log.info("所有数据都能够被找到")
    }

    private fun testWhileInsertAndRetrive(url: Url) {
        val accessChord = AccessChord()
        accessChord.join(urlx, idx, url)
        val random = ThreadLocalRandom.current()
        for (i in 1.until(50)) {
            val v = random.nextInt(1000)
            val strKey = "tx$v"
            val value = "tv$v"
            val k = Key.of(strKey)
            val ev = accessChord.get<Any>(k)
            ev?.let {
                println("通过之前已经插入的数据能找到数据.k:$strKey,v:$ev")
            }
            accessChord.insert(k, value)

            Thread.sleep(2000)
        }
        accessChord.leave()
    }

    private fun printEntryAndCheck(chord1: PersistentChord, chord2: PersistentChord, chord3: PersistentChord) {
        if (chord1.isRunning())
            println("e1->" + chord1.reportedNode().printEntries())

        if (chord2.isRunning())
            println("e2->" + chord2.reportedNode().printEntries())

        if (chord3.isRunning())
            println("e3->" + chord3.reportedNode().printEntries())

        //检查数据
        if (chord1.isRunning())
            chord1.reportedNode().checkEntries()

        if (chord2.isRunning())
            chord2.reportedNode().checkEntries()

        if (chord3.isRunning())
            chord3.reportedNode().checkEntries()
    }

    private fun printNodeId() {
        System.out.println("节点a:" + id1.toHexString())
        System.out.println("节点b:" + id2.toHexString())
        System.out.println("节点c:" + id3.toHexString())
        System.out.println("节点x:" + idx.toHexString())
    }

    @Throws(Exception::class)
    @JvmStatic
    fun main(args: Array<String>) {
        System.setProperty("fastjson.parser.autoTypeAccept", "io.iflym")

        PropertiesLoader.loadPropertyFile()

        printNodeId()

        val urls = arrayOf(url1, url2, url3)

        val chord1 = PersistentChord()

        chord1.join(url1, id1, url1, url2, url3)

        //先加1个数据看看

        chord1.insert(Key.of("a1"), "a1")

        var obj1 = chord1.get<Any>(Key.of("a1"))
        println("1->" + obj1)

        val chord2 = PersistentChord()
        chord2.join(url2, id2, url1, url2, url3)

        obj1 = chord2.get(Key.of("a1"))
        println("1_1->" + obj1)

        chord2.insert(Key.of("a2"), "a2")
        obj1 = chord2.get(Key.of("a2"))
        println("2->" + obj1)

        chord2.insert(Key.of("b1"), "b1")
        obj1 = chord2.get(Key.of("b1"))
        println("3->" + obj1)

        val chord3 = PersistentChord()
        chord3.join(url3, id3, url1, url2, url3)

        for (i in 10..29) {
            chord2.insert(Key.of("x" + i), "x" + i)
        }

        println("------------------------------所有的节点都在正常工作")
        println("---------------获取相应的结果信息")
        testRetrive(*urls)

        println("--------------检查相应的各个节点数据")
        printEntryAndCheck(chord1, chord2, chord3)

        println("--------------------准备掉线节点2")

        chord2.leave()

        println("---------------获取相应的结果信息")
        testRetrive(url1, url3)

        println("--------------检查相应的各个节点数据")
        printEntryAndCheck(chord1, chord2, chord3)

        println("---------------准备不停的循环插入和获取处理")
        testWhileInsertAndRetrive(url1)

        println("ok---------------------")
    }
}
