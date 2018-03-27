/*
* The MIT License (MIT)
*
* Copyright (c) 2002 - 2018, Hitachi Vantara
*
* Permission is hereby granted, free of charge, to any person obtaining a copy
* of this software and associated documentation files (the "Software"), to deal
* in the Software without restriction, including without limitation the rights
* to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
* copies of the Software, and to permit persons to whom the Software is
* furnished to do so, subject to the following conditions:
*
* The above copyright notice and this permission notice shall be included in
* all copies or substantial portions of the Software.
*
* THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
* IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
* FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
* AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
* LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
* OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
* THE SOFTWARE.
*
*/
package com.github.baudekin.experiments

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}

import scala.collection.immutable.Map

// Get rid of advanced feature warnings for using Scala 2.11 or greater
// Postfix order support this class uses it for maps
import language.postfixOps


class GenerateRowStreamer(stepId: String,
                          columnNames: List[String],
                          columnTypes: List[String],
                          columnValues: List[String] ) {

  // Map class for structured output stream
  case class MemMap(map: Map[String, String])

  private var memoryStreamMaps: MemoryStream[MemMap] = _
  private var rddStream: DataFrame = _
  private var outputStream: StreamingQuery = _

  // Zip up the column names and values to create key value
  // Map with the column names being the key
  private val mapData = (columnNames zip columnValues) toMap
  private val schemaData = (columnNames zip columnTypes) toMap

  def primeRddStream(): Unit = {

    val spark: SparkSession = {
      SparkSession.builder().getOrCreate()
    }

    // Required to implicit to setup behind the scenes resolutions must
    // be defined before memoryStreamMaps and outputStream
    implicit val isc: SparkContext = {
      spark.sparkContext
    }
    import spark.implicits._
    implicit val sqlCtx: SQLContext = {
      spark.sqlContext
    }

    // Requires the above two functions to resolve the Int encoder and SQL context
    // at runtime. Always watch out for the needs of scala implicits!!!
    // MemoryStream is an memory based stream available in scala but not Java
    memoryStreamMaps = MemoryStream[MemMap]

    // Create structure of the in memory stream. Set is up as individual time windows that are 5 seconds in size and count the number of records received
    // inside of that time window if limit set
    val memStreamDF = this.memoryStreamMaps.toDF()

    // Create the stream and give it the name of the step. Make sure it has the complete output allows for intermediate processing. In the case of limit
    // Use OutputMode append and collect at the end.  Write to the stream based on a time trigger set for every 3 seconds. Must be close to but greater then
    // half the time window. In sures accurate spread of records if there generation is uniform. Note MemoryStream is designed for testing and does not offer
    // full fault recovery.
    this.outputStream = memStreamDF.
      writeStream.
      format("memory").
      queryName(this.stepId).
      outputMode(OutputMode.Append).start()

    // Build select statement
    val sqlValue: String = {
      var sql: String = "SELECT "
      schemaData foreach (entry => sql += "cast(map['" + entry._1 + "'] as " + entry._2 + ") " + entry._1 + ", ")
      sql = sql.substring(0, sql.length - 2)
      sql += " FROM %s".format(stepId)
      // Return sql string
      sql
    }

    // Initial RDD
    this.rddStream = spark.sql(sqlText = sqlValue)
  }


  private def produce(data: MemMap): Unit =  {
    this.memoryStreamMaps.addData(data)
  }

  def processAllPendingAdditions(): Unit =  {
    this.outputStream.processAllAvailable()
  }

  def addRow(): Unit = {
    this.produce(MemMap(this.mapData))
  }

  def addRow(locValues: List[String]): Unit = {
    val dataMap = (columnNames zip locValues) toMap
    val data = MemMap(dataMap)
    this.produce(data)
  }

  def getRddStream: DataFrame = {
    // Return a copy not the original
    rddStream.toDF()
  }

  def waitOnStreamToTerminate(): Unit = {
    outputStream.awaitTermination()
  }

  def waitOnStreamToTerminate(seconds: Long): Unit = {
    outputStream.awaitTermination(seconds)
  }
}