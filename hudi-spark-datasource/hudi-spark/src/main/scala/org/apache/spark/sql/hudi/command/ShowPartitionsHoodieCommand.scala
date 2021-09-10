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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.{FileInputFormat, JobConf}
import org.apache.hudi.common.engine.HoodieLocalEngineContext
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.hadoop.HoodieParquetInputFormat
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.hudi.HoodieSqlUtils.getTableLocation
import org.apache.spark.sql.types.StringType

import scala.collection.JavaConverters._

/**
 * A command to list the partition names of a table.
 */
case class ShowPartitionsHoodieCommand(
    tableName: TableIdentifier,
    spec: Option[TablePartitionSpec])
  extends RunnableCommand with Logging {

  override val output: Seq[Attribute] = {
    AttributeReference("partition", StringType, nullable = false)() :: Nil
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    val table = catalog.getTableMetadata(tableName)
    val basePath = getTableLocation(table, sparkSession)
    val hadoopConf = sparkSession.sessionState.newHadoopConf()

    if (table.partitionColumnNames.isEmpty) {
      listFiles(hadoopConf, basePath)
    } else {
      if (spec.isDefined) {
        val badColumns = spec.get.keySet.filterNot(table.partitionColumnNames.contains)
        if (badColumns.nonEmpty) {
          val badCols = badColumns.mkString("[", ", ", "]")
          throw new AnalysisException(
            s"Non-partitioning column(s) $badCols are specified for SHOW PARTITIONS")
        }

        val partSpec = spec.get.map { case (key, value) => s"${key}=${value}" }.mkString("/")
        val partitionPath = basePath + "/" + partSpec
        listFiles(hadoopConf, partitionPath)
      } else {
        val metaClient = HoodieTableMetaClient.builder.setConf(hadoopConf).setBasePath(basePath).setLoadActiveTimelineOnLoad(true).build

        val engineContext = new HoodieLocalEngineContext(metaClient.getHadoopConf)
        val partitions = FSUtils.getAllPartitionPaths(engineContext, basePath, true, true, false)

        partitions.asScala.map(Row(_))
      }
    }
  }

  private def listFiles(hadoopConf: Configuration, basePath: String): Seq[Row] = {
    val hoodieInputFormat: HoodieParquetInputFormat = new HoodieParquetInputFormat
    val jobConf: JobConf = new JobConf(hadoopConf)
    jobConf.set("hoodie.metadata.enable", "true")
    hoodieInputFormat.setConf(jobConf)

    logInfo("list path: " + basePath)
    FileInputFormat.setInputPaths(jobConf, basePath)
    hoodieInputFormat.listStatus(jobConf).map(file => file.getPath.toString).map(Row(_))
  }
}
