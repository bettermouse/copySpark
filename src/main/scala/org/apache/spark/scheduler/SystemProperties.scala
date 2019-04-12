package org.apache.spark.scheduler

import scala.collection.{ mutable, Iterator }
import scala.collection.JavaConverters._
import java.security.AccessControlException
import scala.language.implicitConversions


/** A bidirectional map wrapping the java System properties.
  *  Changes to System properties will be immediately visible in the map,
  *  and modifications made to the map will be immediately applied to the
  *  System properties.  If a security manager is in place which prevents
  *  the properties from being read or written, the AccessControlException
  *  will be caught and discarded.
  *
  *  @author Paul Phillips
  *  @version 2.9
  *  @since   2.9
  */
class SystemProperties
  extends mutable.AbstractMap[String, String]
    with mutable.Map[String, String] {

  override def empty = new SystemProperties
  override def default(key: String): String = null

  def iterator: Iterator[(String, String)] =
    wrapAccess(System.getProperties().asScala.iterator) getOrElse Iterator.empty
  def get(key: String) =
    wrapAccess(Option(System.getProperty(key))) flatMap (x => x)
  override def contains(key: String) =
    wrapAccess(super.contains(key)) exists (x => x)

  def -= (key: String): this.type = { wrapAccess(System.clearProperty(key)) ; this }
  def += (kv: (String, String)): this.type = { wrapAccess(System.setProperty(kv._1, kv._2)) ; this }

  def wrapAccess[T](body: => T): Option[T] =
    try Some(body) catch { case _: AccessControlException => None }
}

/** The values in SystemProperties can be used to access and manipulate
  *  designated system properties.  See `scala.sys.Prop` for particulars.
  *  @example {{{
  *    if (!headless.isSet) headless.enable()
  *  }}}
  */


