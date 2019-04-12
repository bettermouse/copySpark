package cn.itcast.implic

import Context._
/**
  * Created by root on 2016/5/13.
  */
//所有的隐式值和隐式方法必须放到object
object Context {
  implicit val aaaaa = "laozhao"
  implicit val i = 1
}

object ImplicitValue {


  def sayHi()(implicit name: String = "laoduan"): Unit = {
    println(s"hi~ $name")
  }

  def main(args: Array[String]) {

    println(1 to 10)

    sayHi()
  }

}
