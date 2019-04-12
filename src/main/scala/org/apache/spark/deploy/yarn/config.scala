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

package org.apache.spark.deploy

package object yarn {

  private[spark] val DRIVER_CORES = "spark.driver.cores"
  private[spark] val AM_MEMORY = "spark.yarn.am.memory"
  private[yarn] val YARN_EXECUTOR_RESOURCE_TYPES_PREFIX = "spark.yarn.executor.resource."
  private[yarn] val YARN_DRIVER_RESOURCE_TYPES_PREFIX = "spark.yarn.driver.resource."
  private[yarn] val YARN_AM_RESOURCE_TYPES_PREFIX = "spark.yarn.am.resource."
  private[spark] val AM_CORES = "spark.yarn.am.cores"
  private[spark] val EXECUTOR_ATTEMPT_FAILURE_VALIDITY_INTERVAL_MS ="spark.yarn.executor.failuresValidityInterval"
}
