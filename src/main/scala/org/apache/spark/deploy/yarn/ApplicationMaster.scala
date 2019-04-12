package org.apache.spark.deploy.yarn

import java.io.File
import java.net.{URI, URL}

import akka.actor.{Actor, ActorRef, ActorSelection, ActorSystem, Props}
import akka.remote.{DisassociatedEvent, RemotingLifecycleEvent}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.{ConverterUtils, Records}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.scheduler.YarnSchedulerBackend
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.RegisterClusterManager
import org.apache.spark.util.{MutableURLClassLoader, Utils}
import org.apache.spark.{SparkConf, SparkEnv}

import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.control.NonFatal

private[spark] class ApplicationMaster(args: ApplicationMasterArguments) extends Logging {

  // Fields used in client mode.
  private var actorSystem: ActorSystem = null
  private var actor: ActorRef = _

  private var reporterThread: Thread = _

  private val isClusterMode = args.userClass != null
  @volatile private var exitCode = 0
  @volatile private var finished = false

  private val sparkConf = new SparkConf()
  if (args.propertiesFile != null) {
    Utils.getPropertiesFromFile(args.propertiesFile).foreach { case (k, v) =>
      sparkConf.set(k, v)
    }
  }
  // A flag to check whether user has initialized spark context
  @volatile private var registered = false
  @volatile private var allocator: YarnAllocator = _

  // Set system properties for each config entry. This covers two use cases:
  // - The default configuration stored by the SparkHadoopUtil class
  // - The user application creating a new SparkConf in cluster mode
  //
  // Both cases create a new SparkConf object which reads these configs from system properties.
  sparkConf.getAll.foreach { case (k, v) =>
    sys.props(k) = v
  }

  private val yarnConf = new YarnConfiguration(SparkHadoopUtil.newConfiguration(sparkConf))

  private val userClassLoader = {
    val classpath = Some(new URI(Client.APP_JAR_NAME)).toArray
    val urls = classpath.map { entry =>
      new URL("file:" + new File(entry.getPath()).getAbsolutePath())
    }
    new MutableURLClassLoader(urls, Utils.getContextOrSparkClassLoader)
  }

  private val client = new YarnRMClient()

  def main(args: Array[String]): Unit = {
    run()
  }

  private val maxNumExecutorFailures = 3

  // 加载客户端设置的本地化文件列表。 这在启动执行程序时使用，并在此处加载，以便这些配置不会在群集模式下污染Web UI的环境页面。
  // 不仅要学习rpc类 还要学习spark的配置
  private val localResources =  {
    logInfo("Preparing Local resources")
    val resources = mutable.HashMap[String, LocalResource]()

    def setupDistributedCache(
                               file: String,
                               rtype: LocalResourceType,
                               timestamp: String,
                               size: String,
                               vis: String): Unit = {
      val uri = new URI(file)
      val amJarRsrc = Records.newRecord(classOf[LocalResource])
      amJarRsrc.setType(rtype)
      amJarRsrc.setVisibility(LocalResourceVisibility.valueOf(vis))
      amJarRsrc.setResource(ConverterUtils.getYarnUrlFromURI(uri))
      amJarRsrc.setTimestamp(timestamp.toLong)
      amJarRsrc.setSize(size.toLong)

      val fileName = Option(uri.getFragment()).getOrElse(new Path(uri).getName())
      resources(fileName) = amJarRsrc
    }

    //文件　
    val distFiles = sparkConf.get(CACHED_FILES)
    //大小
    val fileSizes = sparkConf.get(CACHED_FILES_SIZES)
    //时间
    val timeStamps = sparkConf.get(CACHED_FILES_TIMESTAMPS)
    //可见性
    val visibilities = sparkConf.get(CACHED_FILES_VISIBILITIES)
    //类型
    val resTypes = sparkConf.get(CACHED_FILES_TYPES)

    for (i <- 0 to distFiles.size - 1) {
      val resType = LocalResourceType.valueOf(resTypes(i))
      setupDistributedCache(distFiles(i), resType, timeStamps(i).toString, fileSizes(i).toString,
        visibilities(i))
    }

    // Distribute the conf archive to executors.
    sparkConf.get(CACHED_CONF_ARCHIVE).foreach { path =>
      val uri = new URI(path)
      val fs = FileSystem.get(uri, yarnConf)
      val status = fs.getFileStatus(new Path(uri))
      // SPARK-16080: Make sure to use the correct name for the destination when distributing the
      // conf archive to executors.
      val destUri = new URI(uri.getScheme(), uri.getRawSchemeSpecificPart(),
        Client.LOCALIZED_CONF_DIR)
      setupDistributedCache(destUri.toString(), LocalResourceType.ARCHIVE,
        status.getModificationTime().toString, status.getLen.toString,
        LocalResourceVisibility.PRIVATE.name())
    }

    // Clean up the configuration so it doesn't show up in the Web UI (since it's really noisy).
//    CACHE_CONFIGS.foreach { e =>
//      sparkConf.remove(e)
//      sys.props.remove(e.key)
//    }
    resources.toMap
  }


  def getAttemptId(): ApplicationAttemptId = {
    client.getAttemptId()
  }

  final def run(): Int = {
      runImpl()
     exitCode
  }

  private def runImpl(): Unit = {
    try {
      val appAttemptId = client.getAttemptId()

      var attemptID: Option[String] = None
      logInfo("ApplicationAttemptId: " + appAttemptId)
      runExecutorLauncher()
    } catch {
      case e: Exception =>
        // catch everything else if not specifically handled
        logError("Uncaught exception: ", e)
    } finally {
    }
  }

  private def runExecutorLauncher(): Unit = {
    actorSystem = AkkaUtils.createActorSystem("sparkYarnAM", Utils.localHostName, 0,
      conf = sparkConf)._1
    //需要传入driver actor的ip和端口
    val (driverHost, driverPort) = Utils.parseHostPort(args.userArgs(0))
    sparkConf.set("spark.driver.host", driverHost)
    sparkConf.set("spark.driver.port", driverPort.toString)
    runAMActor(driverHost, driverPort.toString, isClusterMode = false)
    //Await.result(actorSystem.whenTerminated, Duration.Inf)
    val hostname = Utils.localHostName
    registerAM(hostname, -1, sparkConf, sparkConf.getOption("spark.driver.appUIAddress"))
    allocator.allocateResources()
    reporterThread = launchReporterThread()
    // In client mode the actor will stop the reporter thread.
    reporterThread.join()
  }

  private def runAMActor(
                          host: String,
                          port: String,
                          isClusterMode: Boolean): Unit = {
    val driverUrl = AkkaUtils.address(
      AkkaUtils.protocol(actorSystem),
      SparkEnv.driverActorSystemName,
      host,
      port,
      YarnSchedulerBackend.ACTOR_NAME)
    actor = actorSystem.actorOf(Props(new AMActor(driverUrl, isClusterMode)), name = "YarnAM")
  }
  private def registerAM(
                          host: String,
                          port: Int,
                          _sparkConf: SparkConf,
                          uiAddress: Option[String]): Unit = {
//    val appId = client.getAttemptId().getApplicationId().toString()
//    val attemptId = client.getAttemptId().getAttemptId().toString()
    val historyAddress = ""
    //向ResourceManager注册任务
    logInfo("Registering the ApplicationMaster")
    client.register(host, port, yarnConf, _sparkConf, uiAddress, historyAddress)
    registered = true

    //crete Allocator
    // executor 连 driver地址
    val driverUrl = AkkaUtils.address(
      AkkaUtils.protocol(actorSystem),
      SparkEnv.driverActorSystemName,
      sparkConf.get("spark.driver.host"),
      sparkConf.get("spark.driver.port"),
      YarnSchedulerBackend.DRIVER_ACTOR_NAME)
    val driverRef = actorSystem.actorSelection(driverUrl)
    allocator = client.createAllocator(yarnConf,_sparkConf,driverUrl,driverRef,localResources)
    // todo allocator resource and wait
    //allocator.allocate()

  }

  private def createAllocator( _sparkConf: SparkConf): Unit = {
    val appId = client.getAttemptId().getApplicationId().toString()
/*    val driverUrl = RpcEndpointAddress(driverRef.address.host, driverRef.address.port,
      "CoarseGrainedScheduler").toString*/

    // Before we initialize the allocator, let's log the information about how executors will
    // be run up front, to avoid printing this out for every single executor being launched.
    // Use placeholders for information that changes such as executor IDs.
//    logInfo {
//      val executorMemory = _sparkConf.get(EXECUTOR_MEMORY).toInt
//      val executorCores = _sparkConf.get(EXECUTOR_CORES)
//      val dummyRunner = new ExecutorRunnable(None, yarnConf, _sparkConf, driverUrl, "<executorId>",
//        "<hostname>", executorMemory, executorCores, appId, securityMgr, localResources)
//      dummyRunner.launchContextDebugInfo()
//    }

//    allocator = client.createAllocator(
//      yarnConf,
//      _sparkConf,
//      "",
//     // driverRef,
//      localResources)

    // Initialize the AM endpoint *after* the allocator has been initialized. This ensures
    // that when the driver sends an initial executor request (e.g. after an AM restart),
    // the allocator is ready to service requests.

   //allocator.allocateResources()
    //todo
    // reporterThread = launchReporterThread()
  }

  /**
    * An actor that communicates with the driver's scheduler backend.
    */
  private class AMActor(driverUrl: String, isClusterMode: Boolean) extends Actor {
    var driver: ActorSelection = _

    override def preStart() = {
      logInfo("Listen to driver: " + driverUrl)
      driver = context.actorSelection(driverUrl)
      logInfo("connected driver"+driver)
      // Send a hello message to establish the connection, after which
      // we can monitor Lifecycle Events.
      driver ! "Hello"
      driver ! RegisterClusterManager
      // In cluster mode, the AM can directly monitor the driver status instead
      // of trying to deduce it from the lifecycle of the driver's actor
      if (!isClusterMode) {
        context.system.eventStream.subscribe(self, classOf[RemotingLifecycleEvent])
      }
    }

    override def receive = {
      case x: DisassociatedEvent =>
        logInfo(s"Driver terminated or disconnected! Shutting down. $x")
        // In cluster mode, do not rely on the disassociated event to exit
        // This avoids potentially reporting incorrect exit codes if the driver fails
        // 如果与driver的连接断开,也需要关闭applicationMaster
        System.exit(0)
        if (!isClusterMode) {
          System.exit(0)
         // finish(FinalApplicationStatus.SUCCEEDED, ApplicationMaster.EXIT_SUCCESS)
        }

//      case x: AddWebUIFilter =>
//        logInfo(s"Add WebUI Filter. $x")
//        driver ! x
//
//      case RequestExecutors(requestedTotal) =>
//        Option(allocator) match {
//          case Some(a) => a.requestTotalExecutors(requestedTotal)
//          case None => logWarning("Container allocator is not ready to request executors yet.")
//        }
//        sender ! true
//
//      case KillExecutors(executorIds) =>
//        logInfo(s"Driver requested to kill executor(s) ${executorIds.mkString(", ")}.")
//        Option(allocator) match {
//          case Some(a) => executorIds.foreach(a.killExecutor)
//          case None => logWarning("Container allocator is not ready to kill executors yet.")
//        }
//        sender ! true
    }
  }

  private def launchReporterThread(): Thread = {
    // Ensure that progress is sent before YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS elapses.
    val expiryInterval = yarnConf.getInt(YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS, 120000)

    // we want to be reasonably responsive without causing too many requests to RM.
    val schedulerInterval =
      sparkConf.getLong("spark.yarn.scheduler.heartbeat.interval-ms", 5000)

    // must be <= expiryInterval / 2.
    val interval = math.max(0, math.min(expiryInterval / 2, schedulerInterval))

    // The number of failures in a row until Reporter thread give up
    val reporterMaxFailures = sparkConf.getInt("spark.yarn.scheduler.reporterThread.maxFailures", 5)

    val t = new Thread {
      override def run() {
        var failureCount = 0
        while (!finished) {
          try {
//            if (allocator.getNumExecutorsFailed >= maxNumExecutorFailures) {
//              finish(FinalApplicationStatus.FAILED,
//                ApplicationMaster.EXIT_MAX_EXECUTOR_FAILURES,
//                "Max number of executor failures reached")
//            } else {
//              logDebug("Sending progress")
//              allocator.allocateResources()
//            }
//            failureCount = 0
            allocator.allocateResources()
          } catch {
            case i: InterruptedException =>
            case e: Throwable => {
              failureCount += 1
              if (!NonFatal(e) || failureCount >= reporterMaxFailures) {
//                finish(FinalApplicationStatus.FAILED,
//                  ApplicationMaster.EXIT_REPORTER_FAILURE, "Exception was thrown " +
//                    s"${failureCount} time(s) from Reporter thread.")

              } else {
                logWarning(s"Reporter thread fails ${failureCount} time(s) in a row.", e)
              }
            }
          }
          try {
            Thread.sleep(interval)
          } catch {
            case e: InterruptedException =>
          }
        }
      }
    }
    // setting to daemon status, though this is usually not a good idea.
    t.setDaemon(true)
    t.setName("Reporter")
    t.start()
    logInfo("Started progress reporter thread - sleep time : " + interval)
    t
  }

}

/**
  * This object does not provide any special functionality. It exists so that it's easy to tell
  * apart the client-mode AM from the cluster-mode AM when using tools such as ps or jps.
  */
object ExecutorLauncher {

  def main(args: Array[String]): Unit = {
    val amArgs = new ApplicationMasterArguments(args)
    val master = new ApplicationMaster(amArgs)
    master.main(null);
  }

}

