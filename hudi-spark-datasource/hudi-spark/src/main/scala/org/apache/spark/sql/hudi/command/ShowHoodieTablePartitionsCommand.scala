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
import org.apache.hudi.common.util.PartitionPathEncodeUtils
import org.apache.hudi.hadoop.HoodieParquetInputFormat
import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.catalog.HoodieCatalogTable
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.datasources.PartitioningUtils
import org.apache.spark.sql.hudi.HoodieSqlUtils.getTableLocation
import org.apache.spark.sql.types.StringType

/**
 * Command for show hudi table's partitions.
 */
case class ShowHoodieTablePartitionsCommand(
    tableIdentifier: TableIdentifier,
    specOpt: Option[TablePartitionSpec])
extends HoodieLeafRunnableCommand {

  override val output: Seq[Attribute] = {
    AttributeReference("partition", StringType, nullable = false)() :: Nil
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val hoodieCatalogTable = HoodieCatalogTable(sparkSession, tableIdentifier)
    val catalog = sparkSession.sessionState.catalog
    val table = catalog.getTableMetadata(tableIdentifier)
    val basePath = getTableLocation(table, sparkSession)
    val hadoopConf = sparkSession.sessionState.newHadoopConf()

    val schemaOpt = hoodieCatalogTable.tableSchema
    val partitionColumnNamesOpt = hoodieCatalogTable.tableConfig.getPartitionFields

    if (partitionColumnNamesOpt.isPresent && partitionColumnNamesOpt.get.nonEmpty && schemaOpt.nonEmpty) {
      if (specOpt.isEmpty) {
        val badColumns = specOpt.get.keySet.filterNot(table.partitionColumnNames.contains)
        if (badColumns.nonEmpty) {
          val badCols = badColumns.mkString("[", ", ", "]")
          throw new AnalysisException(
            s"Non-partitioning column(s) $badCols are specified for SHOW PARTITIONS")
        }

        val partSpec = specOpt.get.map { case (key, value) => s"${key}=${value}" }.mkString("/")
        val partitionPath = basePath + "/" + partSpec
        listFiles(hadoopConf, partitionPath)
      } else {
        val spec = specOpt.get
        hoodieCatalogTable.getAllPartitionPaths.filter { partitionPath =>
          val part = PartitioningUtils.parsePathFragment(partitionPath)
          spec.forall { case (col, value) =>
            PartitionPathEncodeUtils.escapePartitionValue(value) == part.getOrElse(col, null)
          }
        }.map(Row(_))
      }
    } else {
      listFiles(hadoopConf, basePath)
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
