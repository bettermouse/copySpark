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

package org.apache.spark.ui

import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler._
import org.apache.spark.status.AppStatusStore
import org.apache.spark.ui.JettyUtils._
import org.apache.spark.ui.exec.ExecutorsTab

/**
  * Top level user interface for a Spark application.
  * Spark应用程序的顶级用户界面。
  */
private[spark] class SparkUI private(
                                      val store: AppStatusStore,
                                      val sc: Option[SparkContext],
                                      val conf: SparkConf,
                                      var appName: String,
                                      val basePath: String,
                                      val startTime: Long)
  extends WebUI(SparkUI.getUIPort(conf),
    conf, basePath, "SparkUI")
    with Logging
    /* with UIRoot*/ {

  val killEnabled = sc.map(_.conf.getBoolean("spark.ui.killEnabled", true)).getOrElse(false)

  var appId: String = _

  private var streamingJobProgressListener: Option[SparkListener] = None

  /** Initialize all components of the server. */
  def initialize(): Unit = {
    //    val jobsTab = new JobsTab(this, store)
    //    attachTab(jobsTab)
    //    val stagesTab = new StagesTab(this, store)
    //    attachTab(stagesTab)
    //    attachTab(new StorageTab(this, store))
    //    attachTab(new EnvironmentTab(this, store))
    attachTab(new ExecutorsTab(this,store))
    addStaticHandler(SparkUI.STATIC_RESOURCE_DIR)
    //创建跳转 handler
    attachHandler(createRedirectHandler("/", "/executors/", basePath = basePath))
    //    //添加 handler,这里的handler是注解的形式
    //    attachHandler(ApiRootResource.getServletHandler(this))
    //
    //    // These should be POST only, but, the YARN AM proxy won't proxy POSTs
    //    attachHandler(createRedirectHandler(
    //      "/jobs/job/kill", "/jobs/", jobsTab.handleKillRequest, httpMethods = Set("GET", "POST")))
    //    attachHandler(createRedirectHandler(
    //      "/stages/stage/kill", "/stages/", stagesTab.handleKillRequest,
    //      httpMethods = Set("GET", "POST")))
  }

  initialize()

  //  def getSparkUser: String = {
  //    try {
  //      Option(store.applicationInfo().attempts.head.sparkUser)
  //        .orElse(store.environmentInfo().systemProperties.toMap.get("user.name"))
  //        .getOrElse("<unknown>")
  //    } catch {
  //      case _: NoSuchElementException => "<unknown>"
  //    }
  //  }

  def getAppName: String = appName

  def setAppId(id: String): Unit = {
    appId = id
  }

  /** Stop the server behind this web interface. Only valid after bind(). */
  override def stop() {
    super.stop()
    logInfo(s"Stopped Spark web UI at $webUrl")
  }

  //  override def withSparkUI[T](appId: String, attemptId: Option[String])(fn: SparkUI => T): T = {
  //    if (appId == this.appId) {
  //      fn(this)
  //    } else {
  //      throw new NoSuchElementException()
  //    }
  //  }
  //
  //  def getApplicationInfoList: Iterator[ApplicationInfo] = {
  //    Iterator(new ApplicationInfo(
  //      id = appId,
  //      name = appName,
  //      coresGranted = None,
  //      maxCores = None,
  //      coresPerExecutor = None,
  //      memoryPerExecutorMB = None,
  //      attempts = Seq(new ApplicationAttemptInfo(
  //        attemptId = None,
  //        startTime = new Date(startTime),
  //        endTime = new Date(-1),
  //        duration = 0,
  //        lastUpdated = new Date(startTime),
  //        sparkUser = getSparkUser,
  //        completed = false,
  //        appSparkVersion = appSparkVersion
  //      ))
  //    ))
  //  }
  //
  //  def getApplicationInfo(appId: String): Option[ApplicationInfo] = {
  //    getApplicationInfoList.find(_.id == appId)
  //  }

  def getStreamingJobProgressListener: Option[SparkListener] = streamingJobProgressListener

  def setStreamingJobProgressListener(sparkListener: SparkListener): Unit = {
    streamingJobProgressListener = Option(sparkListener)
  }

}

private[spark] abstract class SparkUITab(parent: SparkUI, prefix: String)
  extends WebUITab(parent, prefix) {

  def appName: String = parent.appName

}

private[spark] object SparkUI {
  val DEFAULT_PORT = 4040
  val STATIC_RESOURCE_DIR = "org/apache/spark/ui/static"
  val DEFAULT_POOL_NAME = "default"

  def getUIPort(conf: SparkConf): Int = {
    conf.getInt("spark.ui.port", SparkUI.DEFAULT_PORT)
  }

  /**
    * Create a new UI backed by an AppStatusStore.
    */
  def create(
              sc: Option[SparkContext],
              store: AppStatusStore,
              conf: SparkConf,
              appName: String,
              basePath: String,
              startTime: Long): SparkUI = {

    new SparkUI(store, sc, conf, appName, basePath, startTime)
  }

}
