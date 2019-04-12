package kLineData

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import com.typesafe.config.ConfigFactory
import scala.concurrent.duration._

case class SubscribeSecurity(securities: Array[String])

case class SecurityMessage()
case class SendKLine()
case class KLine(security:String,high:BigDecimal)

class KLineDataServer(val host: String, val port: Int) extends Actor {
  var ref: ActorRef = sender()
  var  allSecurities: Array[String] = Array()
  override def preStart(): Unit = {
    println("KLineServer 启动了")
    //导入隐式转换
    import context.dispatcher //使用timer太low了, 可以使用akka的, 使用定时器, 要导入这个包
    context.system.scheduler.schedule(0 millis, 10 millis, self, SendKLine)

  }
  // 用于接收消息
  override def receive: Receive = {
    case SubscribeSecurity(securities) => {
      allSecurities = securities
      ref = sender()
    }
    case SendKLine => {
      allSecurities.foreach(security=>{
        ref ! KLine(security:String,1)
      })

    }
  }
}

object KLineDataServer {
  def main(args: Array[String]) {
    val host = "127.0.0.1"
    val port = 8888
    // 准备配置
    val configStr =
      s"""
         |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
         |akka.remote.netty.tcp.hostname = "$host"
         |akka.remote.netty.tcp.port = "$port"
       """.stripMargin
    val config = ConfigFactory.parseString(configStr)
    //ActorSystem老大，辅助创建和监控下面的Actor，他是单例的
    val actorSystem = ActorSystem("kLineDataServerSystem", config)
    //创建Actor
    val master = actorSystem.actorOf(Props(new KLineDataServer(host, port)), "kLineDataServer")
    actorSystem.awaitTermination()
  }
}
