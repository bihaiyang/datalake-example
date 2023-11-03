package com.bii.kafka.zk

import java.io.IOException
import java.net.{InetAddress, InetSocketAddress, Socket, SocketTimeoutException}

/**
 * @author bihaiyang
 * @since 2023/11/02
 * @desc
 */
object ZkFourLetterWords {
  def sendStat(host: String, port: Int, timeout: Int): Unit = {
    val hostAddress =
      if (host != null) new InetSocketAddress(host, port)
      else new InetSocketAddress(InetAddress.getByName(null), port)
    val sock = new Socket()
    try {
      sock.connect(hostAddress, timeout)
      val outStream = sock.getOutputStream
      outStream.write("stat".getBytes)
      outStream.flush()
    } catch {
      case e: SocketTimeoutException => throw new IOException("Exception while sending 4lw", e)
    } finally {
      sock.close
    }
  }
}