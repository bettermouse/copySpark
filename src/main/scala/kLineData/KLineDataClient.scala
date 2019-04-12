package kLineData

import akka.actor.{Actor, ActorSelection, ActorSystem, Props}
import com.typesafe.config.ConfigFactory


/**
  * Created by root on 2016/5/13.
  */
class KLineDataClient(val masterHost: String, val masterPort: Int, val memory: Int, val cores: Int) extends Actor {

  var kLineDataServer: ActorSelection = _

  override def preStart(): Unit = {
    //跟Master建立连接
    kLineDataServer = context.actorSelection(s"akka.tcp://kLineDataServerSystem@$masterHost:$masterPort/user/kLineDataServer")
    //向Master发送注册消息
    kLineDataServer ! SubscribeSecurity(Array("00700.HK"))
  }

  override def receive: Receive = {
    case KLine(security,price) => {
      //收到k线,处理k线
      println(s"收到k线$security  $price")
      //1. 每个股票任务都拥有一个队列
      //2.
    }
  }
}

object KLineDataClient {
  def main(args: Array[String]) {
    val host = "127.0.0.1"
    val port = 9999
    val masterHost = "127.0.0.1"
    val masterPort = 8888
    val memory = 1
    val cores = 1
    // 准备配置
    val configStr =
      s"""
         |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
         |akka.remote.netty.tcp.hostname = "$host"
         |akka.remote.netty.tcp.port = "$port"
       """.stripMargin
    val config = ConfigFactory.parseString(configStr)
    //ActorSystem老大，辅助创建和监控下面的Actor，他是单例的
    val actorSystem = ActorSystem("kLineDataServer", config)
    actorSystem.actorOf(Props(new KLineDataClient(masterHost, masterPort, memory, cores)), "Worker")
    actorSystem.awaitTermination()
  }
}



