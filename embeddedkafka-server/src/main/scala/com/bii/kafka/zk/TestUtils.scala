package com.bii.kafka.zk

import kafka.server.{KafkaConfig, KafkaServer}
import org.apache.kafka.common.utils.Time

/**
 * @author bihaiyang
 * @since 2023/11/02
 * @desc
 */
object TestUtils {



  /**
   * Create a kafka server instance with appropriate test settings
   * USING THIS IS A SIGN YOU ARE NOT WRITING A REAL UNIT TEST
   *
   * @param config The configuration of the server
   */
  def createServer(config: KafkaConfig, time: Time = Time.SYSTEM): KafkaServer = {
    createServer(config, time, None)
  }

  def createServer(config: KafkaConfig, threadNamePrefix: Option[String]): KafkaServer = {
    createServer(config, Time.SYSTEM, threadNamePrefix)
  }

  def createServer(config: KafkaConfig, time: Time, threadNamePrefix: Option[String]): KafkaServer = {
    createServer(config, time, threadNamePrefix, startup = true)
  }

  def createServer(config: KafkaConfig, time: Time, threadNamePrefix: Option[String], startup: Boolean): KafkaServer = {
    createServer(config, time, threadNamePrefix, startup, enableZkApiForwarding = false)
  }

  def createServer(config: KafkaConfig, time: Time, threadNamePrefix: Option[String],
                   startup: Boolean, enableZkApiForwarding: Boolean) = {
    val server = new KafkaServer(config, time, threadNamePrefix, enableForwarding = enableZkApiForwarding)
    if (startup) server.startup()
    server
  }
}
