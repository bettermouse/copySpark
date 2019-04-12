/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.deploy.yarn

import java.util.Collections
import java.util.concurrent._

import akka.actor.ActorSelection
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.AMRMClient
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.spark.SparkConf
import org.apache.spark.deploy.yarn.YarnSparkHadoopUtil.{ANY_HOST, RM_REQUEST_PRIORITY}
import org.apache.spark.internal.Logging
import org.apache.spark.util.{Clock, SystemClock, ThreadUtils}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}

/**
  * YarnAllocator is charged with requesting containers from the YARN ResourceManager and deciding
  * what to do with containers when YARN fulfills these requests.
  *
  * This class makes use of YARN's AMRMClient APIs. We interact with the AMRMClient in three ways:
  * * Making our resource needs known, which updates local bookkeeping about containers requested.
  * * Calling "allocate", which syncs our local container requests with the RM, and returns any
  * containers that YARN has granted to us.  This also functions as a heartbeat.
  * * Processing the containers granted to us to possibly launch executors inside of them.
  * 处理授予我们的容器，以便可能在其中启动执行程序。
  * The public methods of this class are thread-safe.  All methods that mutate state are
  * synchronized.
  *
  * YarnAllocator 负责从 yarn ResourceManager请求资源,当yarn满足请求的时候决定container做什么
  * 这个类使用yarn AMRMClient APIs,我们用AMRMClient 交互在3个方面
  * 1. 告知我们的  resource needs,更新本地的关于containers请求的记录
  * 2. 调用 allocate,它和RM同步我们本地 container requests,返回 yarn分配给我们的容器.
  * 这也是一个作为心跳的功能
  * 3 处理分配给我们的containers以便在其中启动执行者。
  */
private[yarn] class YarnAllocator(
                                   driverUrl: String,
                                   driverRef: ActorSelection,
                                   conf: YarnConfiguration,
                                   sparkConf: SparkConf,
                                   amClient: AMRMClient[ContainerRequest],
                                   appAttemptId: ApplicationAttemptId,
                                   localResources: Map[String, LocalResource],
                                   resolver: SparkRackResolver,
                                   clock: Clock = new SystemClock)
  extends Logging {

  // Visible for testing.
  //主机 -> Set[container]
  val allocatedHostToContainersMap = new HashMap[String, collection.mutable.Set[ContainerId]]
  // container->主机
  val allocatedContainerToHostMap = new HashMap[ContainerId, String]

  // Containers that we no longer care about. We've either already told the RM to release them or
  // will on the next heartbeat. Containers get removed from this map after the RM tells us they've
  // completed.
  //不再关心的Containers
  private val releasedContainers = Collections.newSetFromMap[ContainerId](
    new ConcurrentHashMap[ContainerId, java.lang.Boolean])

//  private val runningExecutors = Collections.newSetFromMap[String](
//    new ConcurrentHashMap[String, java.lang.Boolean]())

  //private val numExecutorsStarting = new AtomicInteger(0)

  // Used to generate a unique ID per executor
  private var executorIdCounter: Int = 0

  // private[spark] val failureTracker = new FailureTracker(sparkConf, clock)

  //目标executor数量
  @volatile private var targetNumExecutors = 2

  @volatile private var numExecutorsRunning = 0
  //SchedulerBackendUtils.getInitialTargetExecutorNumber(sparkConf)


  // Executor loss reason requests that are pending - maps from executor ID for inquiry to a
  // list of requesters that should be responded to once we find out why the given executor
  // was lost.
  //  private val pendingLossReasonRequests = new HashMap[String, mutable.Buffer[RpcCallContext]]


  // Keep track of which container is running which executor to remove the executors later
  // Visible for testing.
  private[yarn] val executorIdToContainer = new HashMap[String, Container]

//  private var numUnexpectedContainerRelease = 0L
//  private val containerIdToExecutorId = new HashMap[ContainerId, String]

  // Executor memory in MB.
  protected val executorMemory = 1024
  //sparkConf.get(EXECUTOR_MEMORY).toInt
  // Additional memory overhead.
  protected val memoryOverhead: Int = 300
  /*sparkConf.get(EXECUTOR_MEMORY_OVERHEAD).getOrElse(
      math.max((MEMORY_OVERHEAD_FACTOR * executorMemory).toInt, MEMORY_OVERHEAD_MIN)).toInt*/
  // Number of cores per executor.
  protected val executorCores = 1 // sparkConf.get(EXECUTOR_CORES)

  //private val executorResourceRequests = null
  //sparkConf.getAllWithPrefix(config.YARN_EXECUTOR_RESOURCE_TYPES_PREFIX).toMap

  // Resource capability requested for each executor
  private[yarn] val resource: Resource = {
    val resource = Resource.newInstance(
      executorMemory + memoryOverhead , executorCores)
    // ResourceRequestHelper.setResourceRequests(executorResourceRequests, resource)
    logDebug(s"Created resource capability: $resource")
    resource
  }

  private val launcherPool = ThreadUtils.newDaemonCachedThreadPool(
    "ContainerLauncher", 3) //sparkConf.get(CONTAINER_LAUNCH_MAX_THREADS)

  // For testing
  private val launchContainers = sparkConf.getBoolean("spark.yarn.launchContainers", true)


  // A map to store preferred hostname and possible task numbers running on it.
 // private var hostToLocalTaskCounts: Map[String, Int] = Map.empty

  // Number of tasks that have locality preferences in active stages
  private[yarn] var numLocalityAwareTasks: Int = 0

  def getNumReleasedContainers: Int = releasedContainers.size()

  def allocate(): Unit = {
    val resource = Resource.newInstance(
      1024, 1)
    val RM_REQUEST_PRIORITY = Priority.newInstance(1)
    val request = new ContainerRequest(resource, null, null, RM_REQUEST_PRIORITY, true, null)

    amClient.addContainerRequest(request)
    var i = 1
    while (true) {
      val response = amClient.allocate(0.1f)
      println("response " + response.toString)
      val value1 = amClient.getMatchingRequests(RM_REQUEST_PRIORITY, ANY_HOST, resource)
      println("1    "+value1)
      val containers = response.getAllocatedContainers
      Thread.sleep(10000)
      i += 1
      if (i == 6) {
        val resource = Resource.newInstance(
          2048, 2)
        val RM_REQUEST_PRIORITY = Priority.newInstance(1)
        val request = new ContainerRequest(resource, null, null, RM_REQUEST_PRIORITY, true, null)
        amClient.addContainerRequest(request)
        println("start to 请求 2")
      }
      val value2 = amClient.getMatchingRequests(RM_REQUEST_PRIORITY, ANY_HOST, Resource.newInstance(
        2048, 2))
      println("2    "+value2)
      for (container <- containers.asScala) {
        new ExecutorRunnable(
          Some(container),
          conf,
          sparkConf,
          driverUrl,
          "1",
          container.getNodeId.getHost,
          executorMemory,
          executorCores,
          appAttemptId.getApplicationId.toString,
          localResources
        ).run()
      }
    }
  }

  /**
    * Request resources such that, if YARN gives us all we ask for, we'll have a number of containers
    * equal to maxExecutors.
    *
    * Deal with any containers YARN has granted to us by possibly launching executors in them.
    *
    * This must be synchronized because variables read in this method are mutated by other methods.
    */
  def allocateResources(): Unit = synchronized {
    updateResourceRequests()

    val progressIndicator = 0.1f
    //轮询  ResourceManager, 如果没有等待的container请求，作为心跳　
    val allocateResponse = amClient.allocate(progressIndicator)

    val allocatedContainers = allocateResponse.getAllocatedContainers()
    //查询集群中有多少个节点
    //allocatorBlacklistTracker.setNumClusterNodes(allocateResponse.getNumClusterNodes)
    if (allocatedContainers.size > 0) {
      logDebug("Allocated containers: %d. Current executor count: %d. Cluster resources: %s."
        .format(
          allocatedContainers.size,
          numExecutorsRunning,
          allocateResponse.getAvailableResources))

       handleAllocatedContainers(allocatedContainers.asScala)
    }

    val completedContainers = allocateResponse.getCompletedContainersStatuses()
    if (completedContainers.size > 0) {
      logDebug("Completed %d containers".format(completedContainers.size))
      processCompletedContainers(completedContainers.asScala)
      logDebug("Finished processing %d completed containers. Current running executor count: %d."
        .format(completedContainers.size, numExecutorsRunning))
    }
  }

  /**
    * Update the set of container requests that we will sync with the RM based on the number of
    * executors we have currently running and our target number of executors.
    *
    * Visible for testing.
    */
  def updateResourceRequests(): Unit = {
    val numPendingAllocate = getNumPendingAllocate
    val missing = targetNumExecutors - numPendingAllocate - numExecutorsRunning

    if (missing > 0) {
      logInfo(s"Will request $missing executor containers, each with ${resource.getVirtualCores} " +
        s"cores and ${resource.getMemory} MB memory including $memoryOverhead MB overhead")

      for (i <- 0 until missing) {
        val request = new ContainerRequest(resource, null, null, RM_REQUEST_PRIORITY)
        amClient.addContainerRequest(request)
        val nodes = request.getNodes
        val hostStr = if (nodes == null || nodes.isEmpty) "Any" else nodes.asScala.last
        logInfo(s"Container request (host: $hostStr, capability: $resource)")
      }
    } else if (missing < 0) {
      val numToCancel = math.min(numPendingAllocate, -missing)
      logInfo(s"Canceling requests for $numToCancel executor containers")
      //申请的executor多了
      val matchingRequests = amClient.getMatchingRequests(RM_REQUEST_PRIORITY, ANY_HOST, resource)
      if (!matchingRequests.isEmpty) {
        matchingRequests.iterator().next().asScala
          .take(numToCancel).foreach(amClient.removeContainerRequest)
      } else {
        logWarning("Expected to find pending requests, but found none.")
      }
    }
  }

  /**
    * Number of container requests that have not yet been fulfilled.
    */
  def getNumPendingAllocate: Int = getNumPendingAtLocation(ANY_HOST)

  /**
    * Number of container requests at the given location that have not yet been fulfilled.
    */
  private def getNumPendingAtLocation(location: String): Int =
    amClient.getMatchingRequests(RM_REQUEST_PRIORITY, location, resource).asScala.map(_.size).sum

  def getNumExecutorsRunning: Int = numExecutorsRunning

  /**
    * Handle containers granted by the RM by launching executors on them.
    *
    * Due to the way the YARN allocation protocol works, certain healthy race conditions can result
    * in YARN granting containers that we no longer need. In this case, we release them.
    *
    * Visible for testing.
    */
  def handleAllocatedContainers(allocatedContainers: Seq[Container]): Unit = {
//    val containersToUse = new ArrayBuffer[Container](allocatedContainers.size)
//
//    // Match incoming requests by host
//    val remainingAfterHostMatches = new ArrayBuffer[Container]
//    for (allocatedContainer <- allocatedContainers) {
//      matchContainerToRequest(allocatedContainer, allocatedContainer.getNodeId.getHost,
//        containersToUse, remainingAfterHostMatches)
//    }
//
//    // Match remaining by rack
//    val remainingAfterRackMatches = new ArrayBuffer[Container]
//    for (allocatedContainer <- remainingAfterHostMatches) {
//      val rack = RackResolver.resolve(conf, allocatedContainer.getNodeId.getHost).getNetworkLocation
//      matchContainerToRequest(allocatedContainer, rack, containersToUse,
//        remainingAfterRackMatches)
//    }
//
//    // Assign remaining that are neither node-local nor rack-local
//    val remainingAfterOffRackMatches = new ArrayBuffer[Container]
//    for (allocatedContainer <- remainingAfterRackMatches) {
//      matchContainerToRequest(allocatedContainer, ANY_HOST, containersToUse,
//        remainingAfterOffRackMatches)
//    }
//
//    if (!remainingAfterOffRackMatches.isEmpty) {
//      logDebug(s"Releasing ${remainingAfterOffRackMatches.size} unneeded containers that were " +
//        s"allocated to us")
//      for (container <- remainingAfterOffRackMatches) {
//       // internalReleaseContainer(container)
//      }
//    }
//
//    //runAllocatedContainers(containersToUse)
//
//    logInfo("Received %d containers from YARN, launching executors on %d of them."
//      .format(allocatedContainers.size, containersToUse.size))
       runAllocatedContainers(allocatedContainers.to[mutable.ArrayBuffer])
  }

  /**
    * Looks for requests for the given location that match the given container allocation. If it
    * finds one, removes the request so that it won't be submitted again. Places the container into
    * containersToUse or remaining.
    *
    * @param allocatedContainer container that was given to us by YARN
    * @param location           resource name, either a node, rack, or *
    * @param containersToUse    list of containers that will be used
    * @param remaining          list of containers that will not be used
    */
  private def matchContainerToRequest(
                                       allocatedContainer: Container,
                                       location: String,
                                       containersToUse: ArrayBuffer[Container],
                                       remaining: ArrayBuffer[Container]): Unit = {
    // SPARK-6050: certain Yarn configurations return a virtual core count that doesn't match the
    // request; for example, capacity scheduler + DefaultResourceCalculator. So match on requested
    // memory, but use the asked vcore count for matching, effectively disabling matching on vcore
    // count.
    // 申请的资源与得到的资源不一样???
    val matchingResource = Resource.newInstance(allocatedContainer.getResource.getMemory,
      resource.getVirtualCores)
    val matchingRequests = amClient.getMatchingRequests(allocatedContainer.getPriority, location,
      matchingResource)

    // Match the allocation to a request
    if (!matchingRequests.isEmpty) {
      val containerRequest = matchingRequests.get(0).iterator.next
      amClient.removeContainerRequest(containerRequest)
      containersToUse += allocatedContainer
    } else {
      remaining += allocatedContainer
    }
  }

  /**
    * Launches executors in the allocated containers.
    */
  private def runAllocatedContainers(containersToUse: ArrayBuffer[Container]): Unit = {
    for (container <- containersToUse) {
      numExecutorsRunning += 1
      assert(numExecutorsRunning <= targetNumExecutors)
      val executorHostname = container.getNodeId.getHost
      val containerId = container.getId
      executorIdCounter += 1
      val executorId = executorIdCounter.toString

      assert(container.getResource.getMemory >= resource.getMemory)

      logInfo("Launching container %s for on host %s".format(containerId, executorHostname))
      executorIdToContainer(executorId) = container

      val containerSet = allocatedHostToContainersMap.getOrElseUpdate(executorHostname,
        new HashSet[ContainerId])
      containerSet += containerId
      allocatedContainerToHostMap.put(containerId, executorHostname)

      val executorRunnable = new ExecutorRunnable(
        Some(container),
        conf,
        sparkConf,
        driverUrl,
        executorId,
        executorHostname,
        executorMemory,
        executorCores,
        appAttemptId.getApplicationId.toString,
        localResources)
      if (launchContainers) {
        logInfo("Launching ExecutorRunnable. driverUrl: %s,  executorHostname: %s".format(
          driverUrl, executorHostname))
        launcherPool.execute(new Runnable {
          override def run(): Unit = {
            executorRunnable.run()
          }
        })
      }
    }
  }

  // Visible for testing.
  private[yarn] def processCompletedContainers(completedContainers: Seq[ContainerStatus]): Unit = {
    for (completedContainer <- completedContainers) {
      val containerId = completedContainer.getContainerId

      if (releasedContainers.contains(containerId)) {
        // Already marked the container for release, so remove it from
        // `releasedContainers`.
        releasedContainers.remove(containerId)
      } else {
        // Decrement the number of executors running. The next iteration of
        // the ApplicationMaster's reporting thread will take care of allocating.
        numExecutorsRunning -= 1
        logInfo("Completed container %s (state: %s, exit status: %s)".format(
          containerId,
          completedContainer.getState,
          completedContainer.getExitStatus))
        // Hadoop 2.2.X added a ContainerExitStatus we should switch to use
        // there are some exit status' we shouldn't necessarily count against us, but for
        // now I think its ok as none of the containers are expected to exit
//        if (completedContainer.getExitStatus == -103) { // vmem limit exceeded
//          logWarning(memLimitExceededLogMessage(
//            completedContainer.getDiagnostics,
//            VMEM_EXCEEDED_PATTERN))
//        } else if (completedContainer.getExitStatus == -104) { // pmem limit exceeded
//          logWarning(memLimitExceededLogMessage(
//            completedContainer.getDiagnostics,
//            PMEM_EXCEEDED_PATTERN))
//        } else if (completedContainer.getExitStatus != 0) {
//          logInfo("Container marked as failed: " + containerId +
//            ". Exit status: " + completedContainer.getExitStatus +
//            ". Diagnostics: " + completedContainer.getDiagnostics)
//          numExecutorsFailed += 1
//        }
      }

      if (allocatedContainerToHostMap.asJava.containsKey(containerId)) {
        val host = allocatedContainerToHostMap.get(containerId).get
        val containerSet = allocatedHostToContainersMap.get(host).get

        containerSet.remove(containerId)
        if (containerSet.isEmpty) {
          allocatedHostToContainersMap.remove(host)
        } else {
          allocatedHostToContainersMap.update(host, containerSet)
        }
        allocatedContainerToHostMap.remove(containerId)
      }
    }
  }


}
