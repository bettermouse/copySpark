package org.apache.spark.deploy.yarn

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf

class SparkHadoopUtil {
}

object SparkHadoopUtil {
  /**
    * Returns a Configuration object with Spark configuration applied on top. Unlike
    * the instance method, this will always return a Configuration instance, and not a
    * cluster manager-specific type.
    */
  private[spark] def newConfiguration(conf: SparkConf): Configuration = {
    val hadoopConf = new Configuration()
    appendSparkHadoopConfigs(conf, hadoopConf)
    val bufferSize = conf.get("spark.buffer.size", "65536")
    hadoopConf.set("io.file.buffer.size", bufferSize)
    hadoopConf
  }

  /**
    * spark.hadoop.foo=bar  => foo=bar
    * @param conf
    * @param hadoopConf
    */
  private def appendSparkHadoopConfigs(conf: SparkConf, hadoopConf: Configuration): Unit = {
    // Copy any "spark.hadoop.foo=bar" spark properties into conf as "foo=bar"
    for ((key, value) <- conf.getAll if key.startsWith("spark.hadoop.")) {
      hadoopConf.set(key.substring("spark.hadoop.".length), value)
    }
  }

  /**
    * Name of the file containing the gateway's Hadoop configuration, to be overlayed on top of the
    * cluster's Hadoop config. It is up to the Spark code launching the application to create
    * this file if it's desired. If the file doesn't exist, it will just be ignored.
    */
  private[spark] val SPARK_HADOOP_CONF_FILE = "__spark_hadoop_conf__.xml"
}
