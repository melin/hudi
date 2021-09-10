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

package org.apache.spark.sql.hudi.command

import org.apache.hadoop.fs.Path
import org.apache.hudi.HoodieSparkSqlWriter.TableInstantInfo
import org.apache.hudi.client.{HoodieWriteResult, SparkRDDWriteClient}
import org.apache.hudi.common.model.WriteOperationType
import org.apache.hudi.{DataSourceUtils, HoodieWriterUtils, common}
import org.apache.hudi.common.table.timeline.{HoodieActiveTimeline, HoodieTimeline}
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.hudi.HoodieSqlUtils.getTableLocation

import java.util
import scala.collection.JavaConverters._

/**
 * Command for drop partitions the hudi table.
 */
case class AlterHoodieTableDropPartitionCommand(
    tableName: TableIdentifier,
    specs: Seq[TablePartitionSpec],
    ifExists: Boolean,
    purge: Boolean,
    retainData: Boolean)
  extends RunnableCommand with Logging {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    val table = catalog.getTableMetadata(tableName)
    val basePath = getTableLocation(table, sparkSession)

    val jsc = new JavaSparkContext(sparkSession.sparkContext)
    val client = DataSourceUtils.createHoodieClient(jsc, schema.toString,
      basePath, table.identifier.table, HoodieWriterUtils.parametersWithWriteDefaults(table.storage.properties).asJava)

    val partitions = specs.map { spec =>
      spec.toSeq.map {case (key, value) => s"${key}=${value}"}.mkString("/")
    }

    val instantTime = HoodieActiveTimeline.createNewInstantTime()
    val commitActionType = HoodieTimeline.REPLACE_COMMIT_ACTION
    val operation = WriteOperationType.DELETE_PARTITION

    client.startCommitWithTime(instantTime, HoodieTimeline.REPLACE_COMMIT_ACTION)
    logInfo("delete partitions: " + partitions.mkString(","))
    val writeResult = DataSourceUtils.doDeletePartitionsOperation(client, partitions.asJava, instantTime)

    val tableInstantInfo = TableInstantInfo(new Path(basePath), instantTime, commitActionType, operation)
    commit(client, writeResult, tableInstantInfo, basePath)

    Seq.empty[Row]
  }

  private def commit(client: SparkRDDWriteClient[_],
                     writeResult: HoodieWriteResult,
                     tableInstantInfo: TableInstantInfo,
                     basePath: String): Unit = {

    if (writeResult.getWriteStatuses.rdd.filter(ws => ws.hasErrors).isEmpty()) {
      logInfo("Proceeding to commit the write.")
      val commitSuccess =
        client.commit(tableInstantInfo.instantTime, writeResult.getWriteStatuses,
          common.util.Option.of(new util.HashMap[String, String]()),
          tableInstantInfo.commitActionType,
          writeResult.getPartitionToReplaceFileIds)

      if (commitSuccess) {
        writeResult.getPartitionToReplaceFileIds.asScala.toSeq.map {
          case(key, _) => logInfo("delete partition: " + basePath + "/" + key)
        }
        logInfo("Commit " + tableInstantInfo.instantTime + " successful!")
      }
      else {
        logInfo("Commit " + tableInstantInfo.instantTime + " failed!")
      }
    }
  }
}
