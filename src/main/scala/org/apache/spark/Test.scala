package org.apache.spark

import akka.actor.{Actor, ActorRef, Props}
import akka.remote.{DisassociatedEvent, RemotingLifecycleEvent}
import org.apache.spark.scheduler.{SparkContext, YarnClientSchedulerBackend, YarnSchedulerBackend}
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.RegisterClusterManager
import org.apache.spark.util.Utils

import scala.concurrent.Await
import scala.concurrent.duration.Duration


object Test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("yarn")
    val context = new SparkContext(conf)

    while(true){
      Thread.sleep(100000)
      println("用户任务运行中")
    }
  }

  /**
    * An actor that communicates with the ApplicationMaster.
    */
  private class YarnSchedulerActor extends Actor {
    private var amActor: Option[ActorRef] = None

    //    implicit val askAmActorExecutor = ExecutionContext.fromExecutor(
    //      Utils.newDaemonCachedThreadPool("yarn-scheduler-ask-am-executor"))

    override def preStart(): Unit = {
      // Listen for disassociation events
      context.system.eventStream.subscribe(self, classOf[RemotingLifecycleEvent])
    }

    override def receive = {
      case RegisterClusterManager =>
        println("aaa")
    }
  }
}
