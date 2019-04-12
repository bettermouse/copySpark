package cn.itcast.rpc

import java.util.UUID

import akka.actor.{Actor, ActorSelection, ActorSystem, Props}
import akka.remote.{DisassociatedEvent, RemotingLifecycleEvent}
import akka.util.Timeout
import com.typesafe.config.ConfigFactory

import scala.concurrent.Await
import scala.concurrent.duration._

/**
  * Created by root on 2016/5/13.
  */
class Worker(val masterHost: String, val masterPort: Int, val memory: Int, val cores: Int) extends Actor{

  var master : ActorSelection = _
  val workerId = UUID.randomUUID().toString
  val HEART_INTERVAL = 3000

  //
  override def preStart(): Unit = {
    println("start")
    //跟Master建立连接
    context.system.eventStream.subscribe(self, classOf[RemotingLifecycleEvent])
    master = context.actorSelection(s"akka.tcp://MasterSystem@$masterHost:$masterPort/user/Master")
    //向Master发送注册消息
    master ! RegisterWorker(workerId, memory, cores)
  }

  override def receive: Receive = {
    case RegisteredWorker(masterUrl) => {
      println(masterUrl)
      //启动定时器发送心跳
      import context.dispatcher
      //多长时间后执行 单位,多长时间执行一次 单位, 消息的接受者(直接给master发不好, 先给自己发送消息, 以后可以做下判断, 什么情况下再发送消息), 信息
      context.system.scheduler.schedule(0 millis, HEART_INTERVAL millis, self, SendHeartbeat)
    }

    case SendHeartbeat => {
      println("send heartbeat to master")
      import akka.pattern.ask
      master ! Heartbeat(workerId)
      Heartbeat(workerId)
      implicit val timeout = Timeout(600 millis)
      master ? Heartbeat(workerId)
      val bool = Await.result(
        master ? Heartbeat(workerId),
        timeout.duration).asInstanceOf[Boolean]

      println("send heartbeat to master end")
    }

    case x: DisassociatedEvent =>
     println("disassociated")
  }

  override def unhandled(message: Any): Unit = {
    super.unhandled(message)
    println(message)
  }
}

object Worker {
  def main(args: Array[String]) {
    val host = "192.168.83.82"
    val port = 9999
    val masterHost = "192.168.83.88"
    val masterPort = 8888
    val memory = 1
    val cores = 1
    // 准备配置
    //
    val configStr =
      s"""
          |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
          |akka.remote.netty.tcp.hostname = "$host"
         |akka.remote.netty.tcp.port = "$port"
       """.stripMargin
    val config = ConfigFactory.parseString(configStr)
    //ActorSystem老大，辅助创建和监控下面的Actor，他是单例的
    val actorSystem = ActorSystem("WorkerSystem", config)
    actorSystem.actorOf(Props(new Worker(masterHost, masterPort, memory, cores)), "Worker")
    //actorSystem.awaitTermination()
  }
}
