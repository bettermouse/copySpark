
package org.apache.spark.deploy.yarn

import java.io.{File, FileFilter, FileOutputStream, IOException, OutputStreamWriter, FileSystem => _}
import java.net.{InetAddress, URI, UnknownHostException}
import java.nio.charset.StandardCharsets
import java.util.{Locale, Properties}
import java.util.zip.{ZipEntry, ZipOutputStream}

import com.google.common.base.Objects
import com.google.common.io.Files
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs._
import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.{YarnClient, YarnClientApplication}
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException
import org.apache.hadoop.yarn.util.Records
import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet, ListBuffer, Map}
import scala.util.control.NonFatal

private[spark] class Client(
                             val args: ClientArguments,
                             val sparkConf: SparkConf)
  extends Logging {

  import Client._

  private val yarnClient = YarnClient.createYarnClient
  private val hadoopConf = new YarnConfiguration(SparkHadoopUtil.newConfiguration(sparkConf))

  private val isClusterMode = sparkConf.get("spark.submit.deployMode", "client") == "cluster"


  private val distCacheMgr = new ClientDistributedCacheManager()
  // AM related configurations
  //集群模式 AM与driver运行在同一台机器上
  private val amMemory = 1024
  /*if (isClusterMode) {
      sparkConf.get("spark.driver.memory").toInt
    } else {
      sparkConf.get(AM_MEMORY).toInt
    }*/
  private val amCores = 1
  /*if (isClusterMode) {
      sparkConf.get(DRIVER_CORES)
    } else {
      sparkConf.get(AM_CORES)
    }*/

  // Executor related configurations
  private val executorMemory = 1024 //sparkConf.get(EXECUTOR_MEMORY)


  private var appId: ApplicationId = null

  // The app staging dir based on the STAGING_DIR configuration if configured
  // otherwise based on the users home directory.
  private val appStagingBaseDir = "hdfs://node5:9000/user/hadoop"

  /**
    * Submit an application running our ApplicationMaster to the ResourceManager.
    *
    * The stable Yarn API provides a convenience method (YarnClient#createApplication) for
    * creating applications and setting up the application submission context. This was not
    * available in the alpha API.
    */
  def submitApplication(): ApplicationId = {

    var appId: ApplicationId = null
    try {
      yarnClient.init(hadoopConf)
      yarnClient.start()

      logInfo("Requesting a new application from cluster with %d NodeManagers"
        .format(yarnClient.getYarnClusterMetrics.getNumNodeManagers))

      // Get a new application from our RM
      val newApp = yarnClient.createApplication()
      val newAppResponse = newApp.getNewApplicationResponse()
      appId = newAppResponse.getApplicationId()
      // Set up the appropriate contexts to launch our AM
      val containerContext = createContainerLaunchContext(newAppResponse)
      val appContext = createApplicationSubmissionContext(newApp, containerContext)
      // Finally, submit and monitor the application
      logInfo(s"Submitting application $appId to ResourceManager")
      yarnClient.submitApplication(appContext)
      appId
    } catch {
      case e: Throwable =>
        if (appId != null) {
          //cleanupStagingDir(appId)
        }
        throw e
    }
  }


  /**
    * Set up the context for submitting our ApplicationMaster.
    * This uses the YarnClientApplication not available in the Yarn alpha API.
    */
  def createApplicationSubmissionContext(
                                          newApp: YarnClientApplication,
                                          containerContext: ContainerLaunchContext): ApplicationSubmissionContext = {
    val appContext = newApp.getApplicationSubmissionContext
    appContext.setApplicationName(sparkConf.get("spark.app.name", "Spark"))
    appContext.setQueue(sparkConf.get("default"))
    appContext.setAMContainerSpec(containerContext)
    appContext.setApplicationType("SPARK")
    //设置最大重试次数
    appContext.setMaxAppAttempts(1)

    val capability = Records.newRecord(classOf[Resource])
    capability.setMemory(amMemory + 0)
    capability.setVirtualCores(amCores)
    logDebug(s"Created resource capability for AM request: $capability")
    appContext.setResource(capability)
    appContext
    /*    val amResources =HashMap[String,String]()
        logDebug(s"AM resources: $amResources")
        val appContext = newApp.getApplicationSubmissionContext
        appContext.setApplicationName(sparkConf.get("spark.app.name", "Spark"))
        appContext.setQueue("default")
        appContext.setAMContainerSpec(containerContext)
        appContext.setApplicationType("SPARK")
        appContext*/
  }

  /**
    * Set up a ContainerLaunchContext to launch our ApplicationMaster container.
    * This sets up the launch environment, java options, and the command for launching the AM.
    * ContainerLaunchContext  来加载 ApplicationMaster container
    * 设置 environment java options  and the command for launching the AM
    */
  private def createContainerLaunchContext(newAppResponse: GetNewApplicationResponse)
  : ContainerLaunchContext = {
    logInfo("Setting up container launch context for our AM")
    val appId = newAppResponse.getApplicationId
    val appStagingDirPath = new Path(appStagingBaseDir, getAppStagingDir(appId))

    val launchEnv = setupLaunchEnv(appStagingDirPath)
    val localResources = prepareLocalResources(appStagingDirPath, null)
    //    val localResources = HashMap[String, LocalResource]()
    //    val amJarRsrc = Records.newRecord(classOf[LocalResource])
    //    localResources("aa")= amJarRsrc
    val amContainer = Records.newRecord(classOf[ContainerLaunchContext])
    amContainer.setLocalResources(localResources.asJava)
    amContainer.setEnvironment(launchEnv.asJava)

    val javaOpts = ListBuffer[String]()

    // Set the environment variable through a command prefix
    // to append to the existing value of the variable
    var prefixEnv: Option[String] = None

    // Add Xmx for AM memory
    javaOpts += "-Xmx" + amMemory + "m"

    val tmpDir = new Path(Environment.PWD.$$(), YarnConfiguration.DEFAULT_CONTAINER_TEMP_DIR)
    javaOpts += "-Djava.io.tmpdir=" + tmpDir

    // For log4j configuration to reference
    javaOpts += ("-Dspark.yarn.app.container.log.dir=" + ApplicationConstants.LOG_DIR_EXPANSION_VAR)
    val userJar =
      if (args.userJar != null) {
        Seq("--jar", args.userJar)
      } else {
        Nil
      }
    val primaryPyFile =
      if (isClusterMode && args.primaryPyFile != null) {
        Seq("--primary-py-file", new Path(args.primaryPyFile).getName())
      } else {
        Nil
      }
    val primaryRFile =
      if (args.primaryRFile != null) {
        Seq("--primary-r-file", args.primaryRFile)
      } else {
        Nil
      }
    val amClass = "hello"
    /*      if (isClusterMode) {
            Utils.classForName("org.apache.spark.deploy.yarn.ApplicationMaster").getName
          } else {
            Utils.classForName("org.apache.spark.deploy.yarn.ExecutorLauncher").getName
          }*/
    val userArgs = args.userArgs.flatMap { arg =>
      Seq("--arg", YarnSparkHadoopUtil.escapeForShell(arg))
    }
    // Command for the ApplicationMaster
    val commands = prefixEnv ++
      Seq(Environment.JAVA_HOME.$$() + "/bin/java", "-server org.apache.spark.deploy.yarn.ExecutorLauncher") ++
      userArgs ++
      Seq("--properties-file", buildPath(Environment.PWD.$$(), LOCALIZED_CONF_DIR, SPARK_CONF_FILE),
        "1>", ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout",
        "2>", ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr")

    // TODO: it would be nicer to just make sure there are no null commands here
    val printableCommands = commands.map(s => if (s == null) "null" else s).toList
    amContainer.setCommands(printableCommands.asJava)

    logDebug("===============================================================================")
    logDebug("YARN AM launch context:")
    logDebug(s"    user class: ${Option(args.userClass).getOrElse("N/A")}")
    logDebug("    env:")
    launchEnv.foreach { case (k, v) => logDebug(s"        $k -> $v") }

    logDebug("    resources:")
    localResources.foreach { case (k, v) => logDebug(s"        $k -> $v") }
    logDebug("    command:")
    logDebug(s"        ${printableCommands.mkString(" ")}")
    logDebug("===============================================================================")

    // send the acl settings into YARN to control who has access via YARN interfaces
    //val securityManager = new SecurityManager(sparkConf)
    /*    amContainer.setApplicationACLs(
          YarnSparkHadoopUtil.getApplicationAclsForYarn(securityManager).asJava)*/
    //setupSecurityToken(amContainer)
    amContainer
  }

  /**
    * Set up the environment for launching our ApplicationMaster container.
    */
  private def setupLaunchEnv(
                              stagingDirPath: Path): HashMap[String, String] = {
    logInfo("Setting up the launch environment for our AM container")
    val env = new HashMap[String, String]()
    env.+=((Environment.CLASSPATH.name, "/home/hadoop/softBig/scalaJars/*<CPS>{{PWD}}<CPS>{{PWD}}/*<CPS>{{PWD}}/__spark_conf__<CPS>{{PWD}}/__spark_libs__/*"))
    env
  }

  /**
    * Upload any resources to the distributed cache if needed. If a resource is intended to be
    * consumed locally, set up the appropriate config for downstream code to handle it properly.
    * This is used for setting up a container launch context for our ApplicationMaster.
    * Exposed for testing.
    * 如果需要，将任何资源上载到分布式缓存。如果要在本地使用资源，
    * 请为下游代码设置适当的配置以正确处理该资源。
    * 这用于为applicationmaster设置容器启动上下文。暴露测试。
    */
  def prepareLocalResources(
                             destDir: Path,
                             pySparkArchives: Seq[String]): HashMap[String, LocalResource] = {
    logInfo("Preparing resources for our AM container")
    // Upload Spark and the application JAR to the remote file system if necessary,
    // and add them as local resources to the application master.
    //如果必要的话,上传spark和 application jar 到远程的文件系统
    //作为本地资源添加他们到application master
    val fs = destDir.getFileSystem(hadoopConf)

    // Used to keep track of URIs added to the distributed cache. If the same URI is added
    // multiple times, YARN will fail to launch containers for the app with an internal
    // error.
    // 追踪 添加到distributed cache的URIs,如果相同的URI添加多次,yarn将会失败
    val distributedUris = new HashSet[String]
    // Used to keep track of URIs(files) added to the distribute cache have the same name. If
    // same name but different path files are added multiple time, YARN will fail to launch
    // containers for the app with an internal error.
    val distributedNames = new HashSet[String]

    val replication = 1.toShort
    FileSystem.mkdirs(fs, destDir, new FsPermission(STAGING_DIR_PERMISSION))

    val statCache: Map[URI, FileStatus] = HashMap[URI, FileStatus]()
    val symlinkCache: Map[URI, Path] = HashMap[URI, Path]()
    val localResources = HashMap[String, LocalResource]()

    def addDistributedUri(uri: URI): Boolean = {
      val uriStr = uri.toString()
      val fileName = new File(uri.getPath).getName
      if (distributedUris.contains(uriStr)) {
        logWarning(s"Same path resource $uri added multiple times to distributed cache.")
        false
      } else if (distributedNames.contains(fileName)) {
        logWarning(s"Same name resource $uri added multiple times to distributed cache")
        false
      } else {
        distributedUris += uriStr
        distributedNames += fileName
        true
      }
    }

    /*
     * Distribute a file to the cluster.
     * 分发一个文件到集群
     * If the file's path is a "local:" URI, it's actually not distributed. Other files are copied
     * to HDFS (if not already there) and added to the application's distributed cache.
     * 如果文件的路径是一个local,它不是分布式的.复制到hdfs上,添加到应用的分布式cache
     * @param path URI of the file to distribute.
     * @param resType Type of resource being distributed.
     * @param destName Name of the file in the distributed cache.
     * @param targetDir Subdirectory where to place the file.
     * @param appMasterOnly Whether to distribute only to the AM.
     * @return A 2-tuple. First item is whether the file is a "local:" URI. Second item is the
     *         localized path for non-local paths, or the input `path` for local paths.
     *         The localized path will be null if the URI has already been added to the cache.
     *         第一项: 文件是否是一个local URI
     *         第二项: 非本地路径的本地路径
     */
    def distribute(
                    path: String,
                    resType: LocalResourceType = LocalResourceType.FILE,
                    destName: Option[String] = None,
                    targetDir: Option[String] = None,
                    appMasterOnly: Boolean = false): (Boolean, String) = {
      val trimmedPath = path.trim()
      val localURI = Utils.resolveURI(trimmedPath)
      if (localURI.getScheme != LOCAL_SCHEME) {
        if (addDistributedUri(localURI)) {
          val localPath = getQualifiedLocalPath(localURI, hadoopConf)
          val linkname = targetDir.map(_ + "/").getOrElse("") +
            destName.orElse(Option(localURI.getFragment())).getOrElse(localPath.getName())
          val destPath = copyFileToRemote(destDir, localPath, replication, symlinkCache)
          val destFs = FileSystem.get(destPath.toUri(), hadoopConf)
          distCacheMgr.addResource(
            destFs, hadoopConf, destPath, localResources, resType, linkname, statCache,
            appMasterOnly = appMasterOnly)
          (false, linkname)
        } else {
          (false, null)
        }
      } else {
        (true, trimmedPath)
      }
    }
    /*    val jarsArchive = File.createTempFile("__spark_libs__", ".zip",
          new File(Utils.getLocalDir(sparkConf)))
        val jarsStream = new ZipOutputStream(new FileOutputStream(jarsArchive))
        val jarsDir = new File("F:\\workspace\\learn\\quantexecutor\\target")
        try {
          jarsStream.setLevel(0)
          jarsDir.listFiles().foreach { f =>
            if (f.isFile && f.getName.toLowerCase(Locale.ROOT).endsWith(".jar") && f.canRead) {
              jarsStream.putNextEntry(new ZipEntry(f.getName))
              Files.copy(f, jarsStream)
              jarsStream.closeEntry()
            }
          }
        } finally {
          jarsStream.close()
        }

        distribute(jarsArchive.toURI.getPath,
          resType = LocalResourceType.ARCHIVE,
          destName = Some("__spark_libs__"))*/
    //F:\workspace\learn\quantexecutor\target
    // val myJarFile = new File("F:\\workspace\\learn\\quantexecutor\\target\\quant-executor-1.0-SNAPSHOT.jar")
    distribute("F:\\workspace\\hs\\quant-executor\\target\\quant-executor-1.0-SNAPSHOT.jar",
      destName = Some("__chen__.jar"))

    /**
      * Add Spark to the cache. There are two settings that control what files to add to the cache:
      * - if a Spark archive is defined, use the archive. The archive is expected to contain
      * jar files at its root directory.
      * - if a list of jars is provided, filter the non-local ones, resolve globs, and
      * add the found files to the cache.
      *
      * Note that the archive cannot be a "local" URI. If none of the above settings are found,
      * then upload all files found in $SPARK_HOME/jars.
      * 添加spark到cache,有两个控制什么样的文件添加到cache的设置
      * 如果spark archive 定义,用archive
      * 如果一系列的jar提供
      */


    /**
      * Do the same for any additional resources passed in through ClientArguments.
      * Each resource category is represented by a 3-tuple of:
      * (1) comma separated list of resources in this category,
      * (2) resource type, and
      * (3) whether to add these resources to the classpath
      */


    // Update the configuration with all the distributed files, minus the conf archive. The
    // conf archive will be handled by the AM differently so that we avoid having to send
    // this configuration by other means. See SPARK-14602 for one reason of why this is needed.
    distCacheMgr.updateConfiguration(sparkConf)

    // Upload the conf archive to HDFS manually, and record its location in the configuration.
    // This will allow the AM to know where the conf archive is in HDFS, so that it can be
    // distributed to the containers.
    //
    // This code forces the archive to be copied, so that unit tests pass (since in that case both
    // file systems are the same and the archive wouldn't normally be copied). In most (all?)
    // deployments, the archive would be copied anyway, since it's a temp file in the local file
    // system.
    val remoteConfArchivePath = new Path(destDir, LOCALIZED_CONF_ARCHIVE)
    val remoteFs = FileSystem.get(remoteConfArchivePath.toUri(), hadoopConf)
    sparkConf.set("spark.yarn.cache.confArchive", remoteConfArchivePath.toString())

    val localConfArchive = new Path(createConfArchive().toURI())
    copyFileToRemote(destDir, localConfArchive, replication, symlinkCache, force = true,
      destName = Some(LOCALIZED_CONF_ARCHIVE))

    // Manually add the config archive to the cache manager so that the AM is launched with
    // the proper files set up.
    distCacheMgr.addResource(
      remoteFs, hadoopConf, remoteConfArchivePath, localResources, LocalResourceType.ARCHIVE,
      LOCALIZED_CONF_DIR, statCache, appMasterOnly = false)

    // Clear the cache-related entries from the configuration to avoid them polluting the
    // UI's environment page. This works for client mode; for cluster mode, this is handled
    // by the AM.
    //CACHE_CONFIGS.foreach(sparkConf.remove)
    localResources
  }

  /**
    * Copy the given file to a remote file system (e.g. HDFS) if needed.
    * The file is only copied if the source and destination file systems are different or the source
    * scheme is "file". This is used for preparing resources for launching the ApplicationMaster
    * container. Exposed for testing.
    */
  private[yarn] def copyFileToRemote(
                                      destDir: Path,
                                      srcPath: Path,
                                      replication: Short,
                                      symlinkCache: Map[URI, Path],
                                      force: Boolean = false,
                                      destName: Option[String] = None): Path = {
    val destFs = destDir.getFileSystem(hadoopConf)
    val srcFs = srcPath.getFileSystem(hadoopConf)
    var destPath = srcPath
    if (force || !compareFs(srcFs, destFs) || "file".equals(srcFs.getScheme)) {
      destPath = new Path(destDir, destName.getOrElse(srcPath.getName()))
      logInfo(s"Uploading resource $srcPath -> $destPath")
      FileUtil.copy(srcFs, srcPath, destFs, destPath, false, hadoopConf)
      destFs.setReplication(destPath, replication)
      destFs.setPermission(destPath, new FsPermission(APP_FILE_PERMISSION))
    } else {
      logInfo(s"Source and destination file systems are the same. Not copying $srcPath")
    }
    // Resolve any symlinks in the URI path so using a "current" symlink to point to a specific
    // version shows the specific version in the distributed cache configuration
    val qualifiedDestPath = destFs.makeQualified(destPath)
    val qualifiedDestDir = qualifiedDestPath.getParent
    val resolvedDestDir = symlinkCache.getOrElseUpdate(qualifiedDestDir.toUri(), {
      val fc = FileContext.getFileContext(qualifiedDestDir.toUri(), hadoopConf)
      fc.resolvePath(qualifiedDestDir)
    })
    new Path(resolvedDestDir, qualifiedDestPath.getName())
  }

  /**
    * Return whether two URI represent file system are the same
    */
  private[spark] def compareUri(srcUri: URI, dstUri: URI): Boolean = {

    if (srcUri.getScheme() == null || srcUri.getScheme() != dstUri.getScheme()) {
      return false
    }

    val srcAuthority = srcUri.getAuthority()
    val dstAuthority = dstUri.getAuthority()
    if (srcAuthority != null && !srcAuthority.equalsIgnoreCase(dstAuthority)) {
      return false
    }

    var srcHost = srcUri.getHost()
    var dstHost = dstUri.getHost()

    // In HA or when using viewfs, the host part of the URI may not actually be a host, but the
    // name of the HDFS namespace. Those names won't resolve, so avoid even trying if they
    // match.
    if (srcHost != null && dstHost != null && srcHost != dstHost) {
      try {
        srcHost = InetAddress.getByName(srcHost).getCanonicalHostName()
        dstHost = InetAddress.getByName(dstHost).getCanonicalHostName()
      } catch {
        case e: UnknownHostException =>
          return false
      }
    }

    Objects.equal(srcHost, dstHost) && srcUri.getPort() == dstUri.getPort()

  }

  /**
    * Return whether the two file systems are the same.
    */
  protected def compareFs(srcFs: FileSystem, destFs: FileSystem): Boolean = {
    val srcUri = srcFs.getUri()
    val dstUri = destFs.getUri()

    compareUri(srcUri, dstUri)
  }

  /**
    * Create an archive with the config files for distribution.
    *
    * These will be used by AM and executors. The files are zipped and added to the job as an
    * archive, so that YARN will explode it when distributing to AM and executors. This directory
    * is then added to the classpath of AM and executor process, just to make sure that everybody
    * is using the same default config.
    *
    * This follows the order of precedence set by the startup scripts, in which HADOOP_CONF_DIR
    * shows up in the classpath before YARN_CONF_DIR.
    *
    * Currently this makes a shallow copy of the conf directory. If there are cases where a
    * Hadoop config directory contains subdirectories, this code will have to be fixed.
    *
    * The archive also contains some Spark configuration. Namely, it saves the contents of
    * SparkConf in a file to be loaded by the AM process.
    * 使用配置文件创建存档以进行分发。
    * 这将被 AM和executors使用,这些文件被 zip 作为一个archive添加到job,yarn 将展示它,当分发到AM和executors
    * 这个目录最后添加到 am 和executor的进程的classpath.只是为了确保每个人都使用相同的默认配置。
    *
    * 这遵循启动脚本设置的优先顺序,其中HADOOP_CONF_DIR在YARN_CONF_DIR之前的类路径中显示
    *
    * 目前,这是conf目录的shallow copy,如果存在hadoop config 目录包含子目录,这些代码必须被修复
    */
  private def createConfArchive(): File = {
    val hadoopConfFiles = new HashMap[String, File]()

    // SPARK_CONF_DIR shows up in the classpath before HADOOP_CONF_DIR/YARN_CONF_DIR
    //spark_con_dir出现在hadoop_con_dir / yarn_con_dir之前的类路径中
    sys.env.get("SPARK_CONF_DIR").foreach { localConfDir =>
      val dir = new File(localConfDir)
      if (dir.isDirectory) {
        val files = dir.listFiles(new FileFilter {
          override def accept(pathname: File): Boolean = {
            pathname.isFile && pathname.getName.endsWith(".xml")
          }
        })
        files.foreach { f => hadoopConfFiles(f.getName) = f }
      }
    }

    // SPARK-23630: during testing, Spark scripts filter out hadoop conf dirs so that user's
    // environments do not interfere with tests. This allows a special env variable during
    // tests so that custom conf dirs can be used by unit tests.
    val confDirs = Seq("HADOOP_CONF_DIR", "YARN_CONF_DIR") ++
      (if (Utils.isTesting) Seq("SPARK_TEST_HADOOP_CONF_DIR") else Nil)

    confDirs.foreach { envKey =>
      sys.env.get(envKey).foreach { path =>
        val dir = new File(path)
        if (dir.isDirectory()) {
          val files = dir.listFiles()
          if (files == null) {
            logWarning("Failed to list files under directory " + dir)
          } else {
            files.foreach { file =>
              if (file.isFile && !hadoopConfFiles.contains(file.getName())) {
                hadoopConfFiles(file.getName()) = file
              }
            }
          }
        }
      }
    }

    val confArchive = File.createTempFile(LOCALIZED_CONF_DIR, ".zip",
      new File(Utils.getLocalDir(sparkConf)))
    val confStream = new ZipOutputStream(new FileOutputStream(confArchive))

    try {
      confStream.setLevel(0)

      // Upload $SPARK_CONF_DIR/log4j.properties file to the distributed cache to make sure that
      // the executors will use the latest configurations instead of the default values. This is
      // required when user changes log4j.properties directly to set the log configurations. If
      // configuration file is provided through --files then executors will be taking configurations
      // from --files instead of $SPARK_CONF_DIR/log4j.properties.

      // Also upload metrics.properties to distributed cache if exists in classpath.
      // If user specify this file using --files then executors will use the one
      // from --files instead.
      for {prop <- Seq("log4j.properties", "metrics.properties")
           url <- Option(Utils.getContextOrSparkClassLoader.getResource(prop))
           if url.getProtocol == "file"} {
        val file = new File(url.getPath())
        confStream.putNextEntry(new ZipEntry(file.getName()))
        Files.copy(file, confStream)
        confStream.closeEntry()
      }

      // Save the Hadoop config files under a separate directory in the archive. This directory
      // is appended to the classpath so that the cluster-provided configuration takes precedence.
      confStream.putNextEntry(new ZipEntry(s"$LOCALIZED_HADOOP_CONF_DIR/"))
      confStream.closeEntry()
      hadoopConfFiles.foreach { case (name, file) =>
        if (file.canRead()) {
          confStream.putNextEntry(new ZipEntry(s"$LOCALIZED_HADOOP_CONF_DIR/$name"))
          Files.copy(file, confStream)
          confStream.closeEntry()
        }
      }

      // Save the YARN configuration into a separate file that will be overlayed on top of the
      // cluster's Hadoop conf.
      confStream.putNextEntry(new ZipEntry(SparkHadoopUtil.SPARK_HADOOP_CONF_FILE))
      hadoopConf.writeXml(confStream)
      confStream.closeEntry()

      // Save Spark configuration to a file in the archive, but filter out the app's secret.
      val props = new Properties()
      sparkConf.getAll.foreach { case (k, v) =>
        props.setProperty(k, v)
      }
      // Override spark.yarn.key to point to the location in distributed cache which will be used
      // by AM.
      //Option(amKeytabFileName).foreach { k => props.setProperty(KEYTAB.key, k) }
      confStream.putNextEntry(new ZipEntry(SPARK_CONF_FILE))
      val writer = new OutputStreamWriter(confStream, StandardCharsets.UTF_8)
      props.store(writer, "Spark configuration.")
      writer.flush()
      confStream.closeEntry()
    } finally {
      confStream.close()
    }
    confArchive
  }


  /** Get the application report from the ResourceManager for an application we have submitted. */
  def getApplicationReport(appId: ApplicationId): ApplicationReport =
    yarnClient.getApplicationReport(appId)

  /**
    * Report the state of an application until it has exited, either successfully or
    * due to some failure, then return a pair of the yarn application state (FINISHED, FAILED,
    * KILLED, or RUNNING) and the final application state (UNDEFINED, SUCCEEDED, FAILED,
    * or KILLED).
    *
    * @param appId                ID of the application to monitor.
    * @param returnOnRunning      Whether to also return the application state when it is RUNNING.
    * @param logApplicationReport Whether to log details of the application report every iteration.
    * @param interval             How often to poll the YARN RM for application status (in ms).
    * @return A pair of the yarn application state and the final application state.
    *         (yarnState,final state)
    */
  def monitorApplication(
                          appId: ApplicationId,
                          returnOnRunning: Boolean = false,
                          logApplicationReport: Boolean = true,
                          interval: Long = 1000L): YarnAppReport = {
    var lastState: YarnApplicationState = null
    while (true) {
      Thread.sleep(interval)
      val report: ApplicationReport =
        try {
          getApplicationReport(appId)
        } catch {
          case e: ApplicationNotFoundException =>
            logError(s"Application $appId not found.")
            //todo 清理目录
            //cleanupStagingDir(appId)
            return YarnAppReport(YarnApplicationState.KILLED, FinalApplicationStatus.KILLED, None)
          case NonFatal(e) =>
            val msg = s"Failed to contact YARN for application $appId."
            logError(msg, e)
            // Don't necessarily clean up staging dir because status is unknown
            return YarnAppReport(YarnApplicationState.FAILED, FinalApplicationStatus.FAILED,
              Some(msg))
        }
      val state = report.getYarnApplicationState

      if (logApplicationReport) {
        logInfo(s"Application report for $appId (state: $state)")
        // If DEBUG is enabled, log report details every iteration
        // Otherwise, log them every time the application changes state
        if (log.isDebugEnabled) {
          logDebug(formatReportDetails(report))
        } else if (lastState != state) {
          logInfo(formatReportDetails(report))
        }
      }

      if (lastState != state) {
        state match {
          case YarnApplicationState.RUNNING =>
          // reportLauncherState(SparkAppHandle.State.RUNNING)
          case YarnApplicationState.FINISHED =>
            report.getFinalApplicationStatus match {
              case FinalApplicationStatus.FAILED =>
              //reportLauncherState(SparkAppHandle.State.FAILED)
              case FinalApplicationStatus.KILLED =>
              // reportLauncherState(SparkAppHandle.State.KILLED)
              case _ =>
              //reportLauncherState(SparkAppHandle.State.FINISHED)
            }
          case YarnApplicationState.FAILED =>
          //reportLauncherState(SparkAppHandle.State.FAILED)
          case YarnApplicationState.KILLED =>
          //reportLauncherState(SparkAppHandle.State.KILLED)
          case _ =>
        }
      }

      if (state == YarnApplicationState.FINISHED ||
        state == YarnApplicationState.FAILED ||
        state == YarnApplicationState.KILLED) {
        //cleanupStagingDir(appId)
        return createAppReport(report)
      }

      if (returnOnRunning && state == YarnApplicationState.RUNNING) {
        return createAppReport(report)
      }

      lastState = state
    }

    // Never reached, but keeps compiler happy
    throw new SparkException("While loop is depleted! This should never happen...")
  }


  private def formatReportDetails(report: ApplicationReport): String = {
    val details = Seq[(String, String)](
      ("client token", getClientToken(report)),
      ("diagnostics", report.getDiagnostics),
      ("ApplicationMaster host", report.getHost),
      ("ApplicationMaster RPC port", report.getRpcPort.toString),
      ("queue", report.getQueue),
      ("start time", report.getStartTime.toString),
      ("final status", report.getFinalApplicationStatus.toString),
      ("tracking URL", report.getTrackingUrl),
      ("user", report.getUser)
    )
    // Use more loggable format if value is null or empty
    details.map { case (k, v) =>
      val newValue = Option(v).filter(_.nonEmpty).getOrElse("N/A")
      s"\n\t $k: $newValue"
    }.mkString("")
  }

  /**
    * Return the security token used by this client to communicate with the ApplicationMaster.
    * If no security is enabled, the token returned by the report is null.
    */
  private def getClientToken(report: ApplicationReport): String =
    Option(report.getClientToAMToken).map(_.toString).getOrElse("")

  def createAppReport(report: ApplicationReport): YarnAppReport = {
    val diags = report.getDiagnostics()
    val diagsOpt = if (diags != null && diags.nonEmpty) Some(diags) else None
    YarnAppReport(report.getYarnApplicationState(), report.getFinalApplicationStatus(), diagsOpt)
  }
}

object Client {

  // Alias for the user jar
  val APP_JAR_NAME: String = "__app__.jar"

  // Distribution-defined classpath to add to processes
  val ENV_DIST_CLASSPATH = "SPARK_DIST_CLASSPATH"

  // Subdirectory where the user's Spark and Hadoop config files will be placed.
  val LOCALIZED_CONF_DIR = "__spark_conf__"

  // Subdirectory in the conf directory containing Hadoop config files.
  val LOCALIZED_HADOOP_CONF_DIR = "__hadoop_conf__"

  // File containing the conf archive in the AM. See prepareLocalResources().
  val LOCALIZED_CONF_ARCHIVE = LOCALIZED_CONF_DIR + ".zip"

  // App files are world-wide readable and owner writable -> rw-r--r--
  val APP_FILE_PERMISSION: FsPermission =
    FsPermission.createImmutable(Integer.parseInt("644", 8).toShort)

  // Staging directory for any temporary jars or files
  val SPARK_STAGING: String = ".sparkStaging"


  // Name of the file in the conf archive containing Spark configuration.
  val SPARK_CONF_FILE = "__spark_conf__.properties"

  // Staging directory is private! -> rwx--------
  val STAGING_DIR_PERMISSION: FsPermission =
    FsPermission.createImmutable(Integer.parseInt("700", 8).toShort)

  // URI scheme that identifies local resources
  val LOCAL_SCHEME = "local"

  /**
    * Return the path to the given application's staging directory.
    */
  private def getAppStagingDir(appId: ApplicationId): String = {
    buildPath(SPARK_STAGING, appId.toString())
  }

  /**
    * Joins all the path components using Path.SEPARATOR.
    */
  def buildPath(components: String*): String = {
    components.mkString(Path.SEPARATOR)
  }

  /**
    * Given a local URI, resolve it and return a qualified local path that corresponds to the URI.
    * This is used for preparing local resources to be included in the container launch context.
    */
  private def getQualifiedLocalPath(localURI: URI, hadoopConf: Configuration): Path = {
    val qualifiedURI =
      if (localURI.getScheme == null) {
        // If not specified, assume this is in the local filesystem to keep the behavior
        // consistent with that of Hadoop
        new URI(FileSystem.getLocal(hadoopConf).makeQualified(new Path(localURI)).toString)
      } else {
        localURI
      }
    new Path(qualifiedURI)
  }


}

private[spark] case class YarnAppReport(
                                         appState: YarnApplicationState,
                                         finalState: FinalApplicationStatus,
                                         diagnostics: Option[String])