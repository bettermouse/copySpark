package org.apache.spark.deploy.yarn

object TestRun {
  def main(args: Array[String]): Unit = {
    while (true){
      Thread.sleep(5000)
      println("i am executor")
    }
  }
}
