package org.apache.spark.scheduler

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.{Actor, ActorRef, ActorSystem, Address, Props}
import akka.remote.{DisassociatedEvent, RemotingLifecycleEvent}
import org.apache.hadoop.yarn.api.records.{ApplicationId, YarnApplicationState}
import org.apache.spark.deploy.yarn.{Client, ClientArguments, YarnAppReport}
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages._
import org.apache.spark.scheduler.cluster.ExecutorData
import org.apache.spark.util.{ActorLogReceive, Utils}
import org.apache.spark.{SparkConf, SparkEnv, SparkException}

import scala.collection.mutable.{ArrayBuffer, HashMap}

class YarnClientSchedulerBackend(env: SparkEnv, conf: SparkConf, actorSystem: ActorSystem) extends Logging {

  // Use an atomic variable to track total number of cores in the cluster for simplicity and speed
  var totalCoreCount = new AtomicInteger(0)
  // Total number of executors that are currently registered
  var totalRegisteredExecutors = new AtomicInteger(0)

  // Number of executors requested from the cluster manager that have not registered yet
  private var numPendingExecutors = 0

  private var client: Client = null
  private var monitorThread: MonitorThread = null
  /** Application ID. */
  protected var appId: Option[ApplicationId] = None

  var driverActor: ActorRef = null
 ////启动 与ApplicationMaster交互的actor
  private val yarnSchedulerActor: ActorRef =
    actorSystem.actorOf(
      Props(new YarnSchedulerActor),
      name = YarnSchedulerBackend.ACTOR_NAME)
  private val executorDataMap = new HashMap[String, ExecutorData]

  /**
    * Create a Yarn client to submit an application to the ResourceManager.
    * This waits until the application is running.
    */
  def start() {
    val properties = new ArrayBuffer[(String, String)]
    for ((key, value) <- conf.getAll) {
      if (key.startsWith("spark.")) {
        properties += ((key, value))
      }
    }
    //先启动driver actor
    driverActor = actorSystem.actorOf(
      Props(new DriverActor(properties)), name = YarnSchedulerBackend.DRIVER_ACTOR_NAME)



    val driverHost = conf.get("spark.driver.host")
    val driverPort = conf.get("spark.driver.port")
    val hostPort = driverHost + ":" + driverPort
    val argsArrayBuf = new ArrayBuffer[String]()
    argsArrayBuf+= ("--arg", hostPort)
    val args = new ClientArguments(argsArrayBuf.toArray)
    client = new Client(args, conf)
    appId = Some(client.submitApplication())
    waitForApplication()
    monitorThread = asyncMonitorApplication()
    monitorThread.start()
  }

  /**
    * Report the state of the application until it is running.
    * If the application has finished, failed or been killed in the process, throw an exception.
    * This assumes both `client` and `appId` have already been set.
    */
  private def waitForApplication(): Unit = {
    val monitorInterval = 1000L //conf.get(CLIENT_LAUNCH_MONITOR_INTERVAL)

    assert(client != null && appId.isDefined, "Application has not been submitted yet!")
    val YarnAppReport(state, _, diags) = client.monitorApplication(appId.get,
      returnOnRunning = true, interval = monitorInterval)
    if (state == YarnApplicationState.FINISHED ||
      state == YarnApplicationState.FAILED ||
      state == YarnApplicationState.KILLED) {
      val genericMessage = "The YARN application has already ended! " +
        "It might have been killed or the Application Master may have failed to start. " +
        "Check the YARN application logs for more details."
      val exceptionMsg = diags match {
        case Some(msg) =>
          logError(genericMessage)
          msg

        case None =>
          genericMessage
      }
      throw new SparkException(exceptionMsg)
    }
    if (state == YarnApplicationState.RUNNING) {
      logInfo(s"Application ${appId.get} has started running.")
    }
  }


  /**
    * Monitor the application state in a separate thread.
    * If the application has exited for any reason, stop the SparkContext.
    * This assumes both `client` and `appId` have already been set.
    * 在单独的线程中监视应用程序状态。 如果应用程序因任何原因退出，
    * 请停止SparkContext。 假设已经设置了client和appId。
    */
  private def asyncMonitorApplication(): MonitorThread = {
    assert(client != null && appId.isDefined, "Application has not been submitted yet!")
    val t = new MonitorThread
    t.setName("YARN application state monitor")
    t.setDaemon(true)
    t
  }

  /**
    * We create this class for SPARK-9519. Basically when we interrupt the monitor thread it's
    * because the SparkContext is being shut down(sc.stop() called by user code), but if
    * monitorApplication return, it means the Yarn application finished before sc.stop() was called,
    * which means we should call sc.stop() here, and we don't allow the monitor to be interrupted
    * before SparkContext stops successfully.
    */
  private class MonitorThread extends Thread {
    private var allowInterrupt = true

    override def run() {
      try {
        val YarnAppReport(_, state, diags) =
          client.monitorApplication(appId.get, logApplicationReport = false)
        logError(s"YARN application has exited unexpectedly with state $state! " +
          "Check the YARN application logs for more details.")
        diags.foreach { err =>
          logError(s"Diagnostics message: $err")
        }
        allowInterrupt = false
        // sc.stop()
      } catch {
        case e: InterruptedException => logInfo("Interrupting monitor thread")
      }
    }

    def stopMonitor(): Unit = {
      if (allowInterrupt) {
        this.interrupt()
      }
    }
  }

  class DriverActor(sparkProperties: Seq[(String, String)]) extends Actor with ActorLogReceive {
    override protected def log = YarnClientSchedulerBackend.this.log

    private val addressToExecutorId = new HashMap[Address, String]

    override def preStart() {
      // Listen for remote client disconnection events, since they don't go through Akka's watch()
      context.system.eventStream.subscribe(self, classOf[RemotingLifecycleEvent])

      // Periodically revive offers to allow delay scheduling to work
      val reviveInterval = conf.getLong("spark.scheduler.revive.interval", 1000)
      //context.system.scheduler.schedule(0.millis, reviveInterval.millis, self, ReviveOffers)
    }

    def receiveWithLogging = {
      case RegisterExecutor(executorId, hostPort, cores, logUrls) =>
        Utils.checkHostPort(hostPort, "Host port expected " + hostPort)
        logInfo("Registered executor: " + sender + " with ID " + executorId)
              if (executorDataMap.contains(executorId)) {
                //如果已经有executor,重复注册
                sender ! RegisterExecutorFailed("Duplicate executor ID: " + executorId)
              } else {
                logInfo("Registered executor: " + sender + " with ID " + executorId)
                sender ! RegisteredExecutor
                addressToExecutorId(sender.path.address) = executorId
                totalCoreCount.addAndGet(cores)
                totalRegisteredExecutors.addAndGet(1)
                val (host, _) = Utils.parseHostPort(hostPort)
                val data = new ExecutorData(sender, sender.path.address, host, cores, cores, logUrls)
                // This must be synchronized because variables mutated
                // in this block are read when requesting executors
               // CoarseGrainedSchedulerBackend.this.synchronized {
                this.synchronized {
                  executorDataMap.put(executorId, data)
                  if (numPendingExecutors > 0) {
                    numPendingExecutors -= 1
                    logDebug(s"Decremented number of pending executors ($numPendingExecutors left)")
                  }
                }
//                listenerBus.post(
//                  SparkListenerExecutorAdded(System.currentTimeMillis(), executorId, data))
//                makeOffers()
              }
      //
      //      case StatusUpdate(executorId, taskId, state, data) =>
      //        scheduler.statusUpdate(taskId, state, data.value)
      //        if (TaskState.isFinished(state)) {
      //          executorDataMap.get(executorId) match {
      //            case Some(executorInfo) =>
      //              executorInfo.freeCores += scheduler.CPUS_PER_TASK
      //              makeOffers(executorId)
      //            case None =>
      //              // Ignoring the update since we don't know about the executor.
      //              logWarning(s"Ignored task status update ($taskId state $state) " +
      //                "from unknown executor $sender with ID $executorId")
      //          }
      //        }
      //
      //      case ReviveOffers =>
      //        makeOffers()
      //
      //      case KillTask(taskId, executorId, interruptThread) =>
      //        executorDataMap.get(executorId) match {
      //          case Some(executorInfo) =>
      //            executorInfo.executorActor ! KillTask(taskId, executorId, interruptThread)
      //          case None =>
      //            // Ignoring the task kill since the executor is not registered.
      //            logWarning(s"Attempted to kill task $taskId for unknown executor $executorId.")
      //        }
      //
      //      case StopDriver =>
      //        sender ! true
      //        context.stop(self)
      //
      //      case StopExecutors =>
      //        logInfo("Asking each executor to shut down")
      //        for ((_, executorData) <- executorDataMap) {
      //          executorData.executorActor ! StopExecutor
      //        }
      //        sender ! true
      //
      //      case RemoveExecutor(executorId, reason) =>
      //        removeExecutor(executorId, reason)
      //        sender ! true
      //
      //      case DisassociatedEvent(_, address, _) =>
      //        addressToExecutorId.get(address).foreach(removeExecutor(_,
      //          "remote Akka client disassociated"))
      //
            case RetrieveSparkProps =>
              sender ! sparkProperties
    }

    //    // Make fake resource offers on all executors
    //    def makeOffers() {
    //      launchTasks(scheduler.resourceOffers(executorDataMap.map { case (id, executorData) =>
    //        new WorkerOffer(id, executorData.executorHost, executorData.freeCores)
    //      }.toSeq))
    //    }
    //
    //    // Make fake resource offers on just one executor
    //    def makeOffers(executorId: String) {
    //      val executorData = executorDataMap(executorId)
    //      launchTasks(scheduler.resourceOffers(
    //        Seq(new WorkerOffer(executorId, executorData.executorHost, executorData.freeCores))))
    //    }
    //
    //    // Launch tasks returned by a set of resource offers
    //    def launchTasks(tasks: Seq[Seq[TaskDescription]]) {
    //      for (task <- tasks.flatten) {
    //        val ser = SparkEnv.get.closureSerializer.newInstance()
    //        val serializedTask = ser.serialize(task)
    //        if (serializedTask.limit >= akkaFrameSize - AkkaUtils.reservedSizeBytes) {
    //          val taskSetId = scheduler.taskIdToTaskSetId(task.taskId)
    //          scheduler.activeTaskSets.get(taskSetId).foreach { taskSet =>
    //            try {
    //              var msg = "Serialized task %s:%d was %d bytes, which exceeds max allowed: " +
    //                "spark.akka.frameSize (%d bytes) - reserved (%d bytes). Consider increasing " +
    //                "spark.akka.frameSize or using broadcast variables for large values."
    //              msg = msg.format(task.taskId, task.index, serializedTask.limit, akkaFrameSize,
    //                AkkaUtils.reservedSizeBytes)
    //              taskSet.abort(msg)
    //            } catch {
    //              case e: Exception => logError("Exception in error callback", e)
    //            }
    //          }
    //        }
    //        else {
    //          val executorData = executorDataMap(task.executorId)
    //          executorData.freeCores -= scheduler.CPUS_PER_TASK
    //          executorData.executorActor ! LaunchTask(new SerializableBuffer(serializedTask))
    //        }
    //      }
    //    }
    //
    //    // Remove a disconnected slave from the cluster
    //    def removeExecutor(executorId: String, reason: String): Unit = {
    //      executorDataMap.get(executorId) match {
    //        case Some(executorInfo) =>
    //          // This must be synchronized because variables mutated
    //          // in this block are read when requesting executors
    //          CoarseGrainedSchedulerBackend.this.synchronized {
    //            addressToExecutorId -= executorInfo.executorAddress
    //            executorDataMap -= executorId
    //            executorsPendingToRemove -= executorId
    //          }
    //          totalCoreCount.addAndGet(-executorInfo.totalCores)
    //          totalRegisteredExecutors.addAndGet(-1)
    //          scheduler.executorLost(executorId, SlaveLost(reason))
    //          listenerBus.post(
    //            SparkListenerExecutorRemoved(System.currentTimeMillis(), executorId, reason))
    //        case None => logError(s"Asked to remove non-existent executor $executorId")
    //      }
    //    }
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
        logInfo(s"ApplicationMaster registered as $sender")
        amActor = Some(sender)

//      case r: RequestExecutors =>
//        amActor match {
//          case Some(actor) =>
//            val driverActor = sender
//            Future {
//              driverActor ! AkkaUtils.askWithReply[Boolean](r, actor, askTimeout)
//            } onFailure {
//              case NonFatal(e) => logError(s"Sending $r to AM was unsuccessful", e)
//            }
//          case None =>
//            logWarning("Attempted to request executors before the AM has registered!")
//            sender ! false
//        }
//
//      case k: KillExecutors =>
//        amActor match {
//          case Some(actor) =>
//            val driverActor = sender
//            Future {
//              driverActor ! AkkaUtils.askWithReply[Boolean](k, actor, askTimeout)
//            } onFailure {
//              case NonFatal(e) => logError(s"Sending $k to AM was unsuccessful", e)
//            }
//          case None =>
//            logWarning("Attempted to kill executors before the AM has registered!")
//            sender ! false
//        }
//
//      case AddWebUIFilter(filterName, filterParams, proxyBase) =>
//        addWebUIFilter(filterName, filterParams, proxyBase)
//        sender ! true

      case d: DisassociatedEvent =>
        if (amActor.isDefined && sender == amActor.get) {
          logWarning(s"ApplicationMaster has disassociated: $d")
        }
    }
  }

}

private[spark] object YarnSchedulerBackend {
  val ACTOR_NAME = "YarnScheduler"
  val DRIVER_ACTOR_NAME = "CoarseGrainedScheduler"
}
