/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.executor

import java.net.URL
import java.util.Properties

import org.apache.spark.deploy.yarn.AkkaUtils
import org.apache.spark.internal.Logging
import org.apache.spark.{Heartbeater, _}

import scala.util.control.NonFatal

/**
 * Spark executor, backed by a threadpool to run tasks.
 *
 * This can be used with Mesos, YARN, and the standalone scheduler.
 * An internal RPC interface is used for communication with the driver,
 * except in the case of Mesos fine-grained mode.
 */
private[spark] class Executor(
    executorId: String,
    executorHostname: String,
    env: SparkEnv,
    userClassPath: Seq[URL] = Nil,
    isLocal: Boolean = false)
  extends Logging {
  // Executor for the heartbeat task.
  val timeout = AkkaUtils.lookupTimeout(env.conf)

  private val heartbeater = new Heartbeater( reportHeartBeat,
    "executor-heartbeater", 1000)
  val heartbeatReceiverRef = AkkaUtils.makeDriverRef(HeartbeatReceiver.ENDPOINT_NAME, env.conf, env.actorSystem)
  heartbeater.start()
  log.error("开始线程")
  /** Reports heartbeat and metrics for active tasks to the driver. */
  private def reportHeartBeat(): Unit = {

    val message = Heartbeat(executorId)
    try {
      log.error("开始心跳")
      val response = AkkaUtils.askWithReply[HeartbeatResponse](message, heartbeatReceiverRef,
        2, 1000, timeout)
    } catch {
      case NonFatal(t) => logWarning("Issue communicating with driver in heartbeater", t)
    }

    Thread.sleep(1000)
  }
}

private[spark] object Executor {
  // This is reserved for internal use by components that need to read task properties before a
  // task is fully deserialized. When possible, the TaskContext.getLocalProperty call should be
  // used instead.
  val taskDeserializationProps: ThreadLocal[Properties] = new ThreadLocal[Properties]
}
