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

package org.apache.spark.ui.exec

import javax.servlet.http.HttpServletRequest
import org.apache.spark.status.AppStatusStore
import org.apache.spark.ui.{SparkUI, SparkUITab, UIUtils, WebUIPage}

import scala.xml.Node

private[ui] class ExecutorsTab(parent: SparkUI,store: AppStatusStore) extends SparkUITab(parent, "executors") {

  init()

  private def init(): Unit = {
    attachPage(new ExecutorsPage(this, store))
  }

}

private[ui] class ExecutorsPage(
                                 parent: SparkUITab,
                                 store: AppStatusStore)
  extends WebUIPage("") {

  def render(request: HttpServletRequest): Seq[Node] = {
    val content =
    //      <div>
    //        {
    //          <div id="active-executors" class="row-fluid"></div> ++
    //          <script src={UIUtils.prependBaseUri(request, "/static/utils.js")}></script> ++
    //          <script src={UIUtils.prependBaseUri(request, "/static/executorspage.js")}></script>
    //        }
    //      </div>
      <div></div>
    UIUtils.headerSparkPage(request, "Executors", content, parent, useDataTables = true)
  }
}
