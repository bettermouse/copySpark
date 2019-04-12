package org.apache.spark

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import org.apache.spark.deploy.yarn.AkkaUtils
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.SparkContext


class SparkEnv(
                val executorId: String,
                val actorSystem: ActorSystem,
                val conf: SparkConf) extends Logging {

}

object SparkEnv extends Logging {
  @volatile private var env: SparkEnv = _

  private[spark] val driverActorSystemName = "sparkDriver"
  private[spark] val executorActorSystemName = "sparkExecutor"

  def set(e: SparkEnv) {
    env = e
  }

  /**
    * Returns the SparkEnv.
    */
  def get: SparkEnv = {
    env
  }


  /**
    * Create a SparkEnv for the driver.
    */
  private[spark] def createDriverEnv(
                                      conf: SparkConf,
                                      isLocal: Boolean): SparkEnv = {
    assert(conf.contains("spark.driver.host"), "spark.driver.host is not set on the driver!")
    assert(conf.contains("spark.driver.port"), "spark.driver.port is not set on the driver!")
    val hostname = conf.get("spark.driver.host")
    val port = conf.get("spark.driver.port").toInt
    create(
      conf,
      SparkContext.DRIVER_IDENTIFIER,
      hostname,
      port,
      isDriver = true,
      isLocal = isLocal
    )
  }

  /**
    * Create a SparkEnv for an executor.
    * In coarse-grained mode, the executor provides an actor system that is already instantiated.
    */
    private[spark] def createExecutorEnv(
                                          conf: SparkConf,
                                          executorId: String,
                                          hostname: String,
                                          port: Int,
                                          numCores: Int,
                                          isLocal: Boolean): SparkEnv = {
      val env = create(
        conf,
        executorId,
        hostname,
        port,
        isDriver = false,
        isLocal = isLocal
      )
      SparkEnv.set(env)
      env
    }

  /**
    * Helper method to create a SparkEnv for a driver or an executor.
    */
  private def create(
                      conf: SparkConf,
                      executorId: String,
                      hostname: String,
                      port: Int,
                      isDriver: Boolean,
                      isLocal: Boolean): SparkEnv = {
    // Create the ActorSystem for Akka and get the port it binds to.
    val (actorSystem, boundPort) = {
      val actorSystemName = if (isDriver) driverActorSystemName else executorActorSystemName
      AkkaUtils.createActorSystem(actorSystemName, hostname, port, conf)
    }

    // Figure out which port Akka actually bound to in case the original port is 0 or occupied.
    if (isDriver) {
      conf.set("spark.driver.port", boundPort.toString)
    } else {
      conf.set("spark.executor.port", boundPort.toString)
    }
    new SparkEnv(
      executorId,
      actorSystem,
      conf)
  }
}