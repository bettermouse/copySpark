package org.apache.spark

import org.apache.hadoop.fs.Path
import org.apache.hadoop.yarn.api.records.{ContainerLaunchContext, LocalResource}
import org.apache.hadoop.yarn.client.api.YarnClient
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.Records
import org.apache.spark.deploy.yarn.{Client, ClientArguments, SparkHadoopUtil}

import scala.collection.mutable.{ArrayBuffer, HashMap, ListBuffer}
import scala.collection.JavaConverters._
object TesetYarn {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val argsArrayBuf = new ArrayBuffer[String]()
    argsArrayBuf += ("--arg", "node5:6666")

    val args = new ClientArguments(argsArrayBuf.toArray)
    val client = new Client(args, conf)
    // val appId = client.submitApplication()
    val hadoopConf = new YarnConfiguration(SparkHadoopUtil.newConfiguration(conf))
    val yarnClient = YarnClient.createYarnClient
    yarnClient.init(hadoopConf)
    yarnClient.start()
    val newApp = yarnClient.createApplication()
    val newAppResponse = newApp.getNewApplicationResponse()
    val appId = newAppResponse.getApplicationId()
    new Path("hdfs://node5:9000/user/hadoop/.sparkStaging/chen_appMaster")

    val env = new HashMap[String, String]()
    val amContainer = Records.newRecord(classOf[ContainerLaunchContext])
    val localResources = HashMap[String, LocalResource]()
    //conf jar directory
    amContainer.setLocalResources(localResources.asJava)
    amContainer.setEnvironment(env.asJava)
    val javaOpts = ListBuffer[String]()

    // Add Xmx for AM memory
    javaOpts += "-Xmx" + 1024 + "m"

    val appContext = newApp.getApplicationSubmissionContext
    // appContext.setApplicationName(sparkConf.get("spark.app.name", "Spark"))
    //appContext.setQueue(sparkConf.get(QUEUE_NAME))
    appContext.setAMContainerSpec(amContainer)
    appContext.setApplicationType("SPARK")
    yarnClient.submitApplication(appContext)


  }
}
