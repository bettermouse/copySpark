
package org.apache.spark

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.{Map => JMap}

import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.{ConfigEntry, ConfigProvider, ConfigReader, SparkConfigProvider}
import org.apache.spark.util.Utils

import scala.collection.JavaConverters._

class SparkConf(loadDefaults: Boolean) extends Cloneable with Logging with Serializable {

  /** Create a SparkConf that loads defaults from system properties and the classpath */
  def this() = this(true)

  private val settings = new ConcurrentHashMap[String, String]()
  @transient private lazy val reader: ConfigReader = {
    val _reader = new ConfigReader(new SparkConfigProvider(settings))
    _reader.bindEnv(new ConfigProvider {
      override def get(key: String): Option[String] = Option(getenv(key))
    })
    _reader
  }


  if (loadDefaults) {
    loadFromSystemProperties(false)
  }

  private[spark] def loadFromSystemProperties(silent: Boolean): SparkConf = {
    // Load any spark.* system properties
    for ((key, value) <- Utils.getSystemProperties if key.startsWith("spark.")) {
      set(key, value, silent)
    }
    this
  }

  /** Set a configuration variable. */
  def set(key: String, value: String): SparkConf = {
    set(key, value, false)
  }

  /** Get a parameter; throws a NoSuchElementException if it's not set */
  def get(key: String): String = {
    settings.get(key)
  }

  /** Get a parameter, falling back to a default if not set */
  def get(key: String, defaultValue: String): String = {
    getOption(key).getOrElse(defaultValue)
  }

  /**
    * Retrieves the value of a pre-defined configuration entry.
    *
    * - This is an internal Spark API.
    * - The return type if defined by the configuration entry.
    * - This will throw an exception is the config is not optional and the value is not set.
    */
  private[spark] def get[T](entry: ConfigEntry[T]): T = {
    entry.readFrom(reader)
  }


  /** Get a parameter as an Option */
  def getOption(key: String): Option[String] = {
    Option(settings.get(key))
  }

  /** Get all parameters as a list of pairs */
  def getAll: Array[(String, String)] = {
    settings.entrySet().asScala.map(x => (x.getKey, x.getValue)).toArray
  }

  /** Get a parameter as an integer, falling back to a default if not set */
  def getInt(key: String, defaultValue: Int): Int = {
    getOption(key).map(_.toInt).getOrElse(defaultValue)
  }

  /** Get a parameter as a long, falling back to a default if not set */
  def getLong(key: String, defaultValue: Long): Long = {
    getOption(key).map(_.toLong).getOrElse(defaultValue)
  }

  /** Get a parameter as a double, falling back to a default if not set */
  def getDouble(key: String, defaultValue: Double): Double = {
    getOption(key).map(_.toDouble).getOrElse(defaultValue)
  }

  /** Get a parameter as a boolean, falling back to a default if not set */
  def getBoolean(key: String, defaultValue: Boolean): Boolean = {
    getOption(key).map(_.toBoolean).getOrElse(defaultValue)
  }

  /** Get all executor environment variables set on this SparkConf */
  def getExecutorEnv: Seq[(String, String)] = {
    val prefix = "spark.executorEnv."
    getAll.filter { case (k, v) => k.startsWith(prefix) }
      .map { case (k, v) => (k.substring(prefix.length), v) }
  }

  /**
    * Returns the Spark application id, valid in the Driver after TaskScheduler registration and
    * from the start in the Executor.
    */
  def getAppId: String = get("spark.app.id")

  /** Does the configuration contain a given parameter? */
  def contains(key: String): Boolean = {
    settings.containsKey(key)
  }

  private[spark] def set(key: String, value: String, silent: Boolean): SparkConf = {
    if (key == null) {
      throw new NullPointerException("null key")
    }
    if (value == null) {
      throw new NullPointerException("null value for " + key)
    }
    if (!silent) {
      //logDeprecationWarning(key)
    }
    settings.put(key, value)
    this
  }

  private[spark] def set[T](entry: ConfigEntry[T], value: T): SparkConf = {
    set(entry.key, entry.stringConverter(value))
    this
  }


  /**
    * Return whether the given config is an akka config (e.g. akka.actor.provider).
    * Note that this does not include spark-specific akka configs (e.g. spark.akka.timeout).
    */
  def isAkkaConf(name: String): Boolean = name.startsWith("akka.")

  /** Get all akka conf variables set on this SparkConf */
  def getAkkaConf: Seq[(String, String)] =
  /* This is currently undocumented. If we want to make this public we should consider
   * nesting options under the spark namespace to avoid conflicts with user akka options.
   * Otherwise users configuring their own akka code via system properties could mess up
   * spark's akka options.
   *
   *   E.g. spark.akka.option.x.y.x = "value"
   */
    getAll.filter { case (k, _) => isAkkaConf(k) }

  /**
    * The master URL to connect to, such as "local" to run locally with one thread, "local[4]" to
    * run locally with 4 cores, or "spark://master:7077" to run on a Spark standalone cluster.
    */
  def setMaster(master: String): SparkConf = {
    set("spark.master", master)
  }

  /** Set a parameter if it isn't already configured */
  def setIfMissing(key: String, value: String): SparkConf = {
    settings.putIfAbsent(key, value)
    this
  }

  /**
    * By using this instead of System.getenv(), environment variables can be mocked
    * in unit tests.
    */
  private[spark] def getenv(name: String): String = System.getenv(name)




}
private[spark] object SparkConf extends Logging {

  private val deprecatedConfigs: Map[String, DeprecatedConfig] = {
    val configs = Seq(
      DeprecatedConfig("spark.files.userClassPathFirst", "spark.executor.userClassPathFirst",
        "1.3"),
      DeprecatedConfig("spark.yarn.user.classpath.first", null, "1.3",
        "Use spark.{driver,executor}.userClassPathFirst instead."))
    configs.map { x => (x.oldName, x) }.toMap
  }

  /**
    * Return whether the given config is an akka config (e.g. akka.actor.provider).
    * Note that this does not include spark-specific akka configs (e.g. spark.akka.timeout).
    */
  def isAkkaConf(name: String): Boolean = name.startsWith("akka.")

  /**
    * Return whether the given config should be passed to an executor on start-up.
    *
    * Certain akka and authentication configs are required of the executor when it connects to
    * the scheduler, while the rest of the spark configs can be inherited from the driver later.
    */
  def isExecutorStartupConf(name: String): Boolean = {
    isAkkaConf(name) ||
      name.startsWith("spark.akka") ||
      name.startsWith("spark.auth") ||
      name.startsWith("spark.ssl") ||
      isSparkPortConf(name)
  }

  /**
    * Return true if the given config matches either `spark.*.port` or `spark.port.*`.
    */
  def isSparkPortConf(name: String): Boolean = {
    (name.startsWith("spark.") && name.endsWith(".port")) || name.startsWith("spark.port.")
  }

  /**
    * Translate the configuration key if it is deprecated and has a replacement, otherwise just
    * returns the provided key.
    *
    * @param userKey Configuration key from the user / caller.
    * @param warn Whether to print a warning if the key is deprecated. Warnings will be printed
    *             only once for each key.
    */
  def translateConfKey(userKey: String, warn: Boolean = false): String = {
    deprecatedConfigs.get(userKey)
      .map { deprecatedKey =>
        if (warn) {
          deprecatedKey.warn()
        }
        deprecatedKey.newName.getOrElse(userKey)
      }.getOrElse(userKey)
  }

  /**
    * Holds information about keys that have been deprecated or renamed.
    *
    * @param oldName Old configuration key.
    * @param newName New configuration key, or `null` if key has no replacement, in which case the
    *                deprecated key will be used (but the warning message will still be printed).
    * @param version Version of Spark where key was deprecated.
    * @param deprecationMessage Message to include in the deprecation warning; mandatory when
    *                           `newName` is not provided.
    */
  private case class DeprecatedConfig(
                                       oldName: String,
                                       _newName: String,
                                       version: String,
                                       deprecationMessage: String = null) {

    private val warned = new AtomicBoolean(false)
    val newName = Option(_newName)

    if (newName == null && (deprecationMessage == null || deprecationMessage.isEmpty())) {
      throw new IllegalArgumentException("Need new config name or deprecation message.")
    }

    def warn(): Unit = {
      if (warned.compareAndSet(false, true)) {
        if (newName != null) {
          val message = Option(deprecationMessage).getOrElse(
            s"Please use the alternative '$newName' instead.")
          logWarning(
            s"The configuration option '$oldName' has been replaced as of Spark $version and " +
              s"may be removed in the future. $message")
        } else {
          logWarning(
            s"The configuration option '$oldName' has been deprecated as of Spark $version and " +
              s"may be removed in the future. $deprecationMessage")
        }
      }
    }

  }
}
