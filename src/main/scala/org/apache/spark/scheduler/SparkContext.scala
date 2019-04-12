package org.apache.spark.scheduler

import akka.actor.Props
import org.apache.spark.status.AppStatusStore
import org.apache.spark.ui.SparkUI
import org.apache.spark.util.Utils
import org.apache.spark.{HeartbeatReceiver, SparkConf, SparkEnv, TaskSchedulerIsSet}

class SparkContext(config: SparkConf) {
  println("开始构建spark context")
  val startTime = System.currentTimeMillis()

  private[spark] val conf = config
  conf.set("spark.app.name", "quant")
  val master = conf.get("spark.master")
  val appName = conf.get("spark.app.name")
  // Set Spark driver host and port system properties
  conf.setIfMissing("spark.driver.host", Utils.localHostName())
  conf.setIfMissing("spark.driver.port", "0")

  val isLocal = (master == "local" || master.startsWith("local["))


  // An asynchronous listener bus for Spark events
  private[spark] val listenerBus = new LiveListenerBus(conf)
  // private var _ui: Option[SparkUI] = None
  private[spark] val env = createSparkEnv(conf, isLocal)
  SparkEnv.set(env)

  private val _statusStore: AppStatusStore = AppStatusStore.createLiveStore(conf)
  listenerBus.addToStatusQueue(_statusStore.listener.get)
  private val _ui = Some(SparkUI.create(Some(this), _statusStore, conf, appName, "",
    startTime))
  _ui.foreach(_.bind())

  Thread.sleep(10000000)
  private val heartbeatReceiver = env.actorSystem.actorOf(
    Props(new HeartbeatReceiver(this)), HeartbeatReceiver.ENDPOINT_NAME)

  val value = new YarnClientSchedulerBackend(env, conf, env.actorSystem)
  value.start()
  heartbeatReceiver ! TaskSchedulerIsSet


  // This function allows components created by SparkEnv to be mocked in unit tests:
  private[spark] def createSparkEnv(
                                     conf: SparkConf,
                                     isLocal: Boolean): SparkEnv = {
    SparkEnv.createDriverEnv(conf, isLocal)
  }


}

object SparkContext {
  /**
    * Executor id for the driver.  In earlier versions of Spark, this was `<driver>`, but this was
    * changed to `driver` because the angle brackets caused escaping issues in URLs and XML (see
    * SPARK-6716 for more details).
    */
  private[spark] val DRIVER_IDENTIFIER = "driver"
}
