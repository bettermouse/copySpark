package org.apache.spark.deploy.yarn

import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.api.records.{ContainerId, Priority}
import org.apache.hadoop.yarn.util.ConverterUtils

object YarnSparkHadoopUtil {

  val ANY_HOST = "*"
  // All RM requests are issued with same priority : we do not (yet) have any distinction between
  // request types (like map/reduce in hadoop for example)
  val RM_REQUEST_PRIORITY = Priority.newInstance(1)
  /**
    *  helper APIs to convert the value obtained from the environment into objects.
    * @return
    */
  def getContainerId: ContainerId = {
    val containerIdString = System.getenv(ApplicationConstants.Environment.CONTAINER_ID.name())
    ConverterUtils.toContainerId(containerIdString)
  }

  /**
    * Escapes a string for inclusion in a command line executed by Yarn. Yarn executes commands
    * using `bash -c "command arg1 arg2"` and that means plain quoting doesn't really work. The
    * argument is enclosed in single quotes and some key characters are escaped.
    *
    * @param arg A single argument.
    * @return Argument quoted for execution via Yarn's generated shell script.
    */
  def escapeForShell(arg: String): String = {
    if (arg != null) {
      val escaped = new StringBuilder("'")
      for (i <- 0 to arg.length() - 1) {
        arg.charAt(i) match {
          case '$' => escaped.append("\\$")
          case '"' => escaped.append("\\\"")
          case '\'' => escaped.append("'\\''")
          case c => escaped.append(c)
        }
      }
      escaped.append("'").toString()
    } else {
      arg
    }
  }

}
