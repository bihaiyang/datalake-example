package com.bii.kafka.zk

import com.bii.kafka.utils.JTestUtils
import kafka.utils.{CoreUtils, Logging}
import org.apache.kafka.common.utils.Utils
import org.apache.zookeeper.server.{NIOServerCnxnFactory, ZooKeeperServer}

import java.io.Closeable
import java.net.InetSocketAddress

/**
 * @author bihaiyang
 * @since 2023/11/02
 * @desc
 */
class EmbeddedZookeeper  extends Closeable with Logging {

  val snapshotDir = JTestUtils.tempDirectory()
  val logDir = JTestUtils.tempDirectory()
  val tickTime = 800 // allow a maxSessionTimeout of 20 * 800ms = 16 secs

  System.setProperty("zookeeper.forceSync", "no")  //disable fsync to ZK txn log in tests to avoid timeout
  val zookeeper = new ZooKeeperServer(snapshotDir, logDir, tickTime)
  val factory = new NIOServerCnxnFactory()
  private val addr = new InetSocketAddress("127.0.0.1", 2181)
  factory.configure(addr, 0)
  factory.startup(zookeeper)
  val port = zookeeper.getClientPort

  def shutdown(): Unit = {
    // Also shuts down ZooKeeperServer
    CoreUtils.swallow(factory.shutdown(), this)

    def isDown(): Boolean = {
      try {
        ZkFourLetterWords.sendStat("127.0.0.1", port, 3000)
        false
      } catch { case _: Throwable => true }
    }

    Iterator.continually(isDown()).exists(identity)
    CoreUtils.swallow(zookeeper.getZKDatabase().close(), this)

    Utils.delete(logDir)
    Utils.delete(snapshotDir)
  }

  override def close(): Unit = shutdown()
}