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

package org.apache.spark.internal

import java.util.concurrent.TimeUnit

import org.apache.spark.network.util.ByteUnit
import org.apache.spark.util.Utils

package object config {

  private[spark] val CACHED_FILES = ConfigBuilder("spark.yarn.cache.filenames")
    .internal()
    .stringConf
    .toSequence
    .createWithDefault(Nil)

  private[spark] val CACHED_FILES_SIZES = ConfigBuilder("spark.yarn.cache.sizes")
    .internal()
    .longConf
    .toSequence
    .createWithDefault(Nil)

  private[spark] val CACHED_FILES_TIMESTAMPS = ConfigBuilder("spark.yarn.cache.timestamps")
    .internal()
    .longConf
    .toSequence
    .createWithDefault(Nil)

  private[spark] val CACHED_FILES_VISIBILITIES = ConfigBuilder("spark.yarn.cache.visibilities")
    .internal()
    .stringConf
    .toSequence
    .createWithDefault(Nil)

  // Either "file" or "archive", for each file.
  private[spark] val CACHED_FILES_TYPES = ConfigBuilder("spark.yarn.cache.types")
    .internal()
    .stringConf
    .toSequence
    .createWithDefault(Nil)

  // The location of the conf archive in HDFS.
  private[spark] val CACHED_CONF_ARCHIVE = ConfigBuilder("spark.yarn.cache.confArchive")
    .internal()
    .stringConf
    .createOptional

  private[spark] val DRIVER_HOST_ADDRESS = ConfigBuilder("spark.driver.host")
    .doc("Address of driver endpoints.")
    .stringConf
    .createWithDefault(Utils.localCanonicalHostName())

  private[spark] val LISTENER_BUS_EVENT_QUEUE_CAPACITY =
    ConfigBuilder("spark.scheduler.listenerbus.eventqueue.capacity")
      .intConf
      .checkValue(_ > 0, "The capacity of listener bus event queue must not be negative")
      .createWithDefault(10000)
}
