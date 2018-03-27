package com.github.baudekin.experiments

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}

import scala.collection.immutable.Map

// Get rid of advanced feature warnings for using Scala 2.11 or greater
// Postfix order support this class uses it for maps
import language.postfixOps


class GenerateRowStreamer(spark: SparkSession,
                          stepId: String,
                          columnNames: List[String],
                          columnTypes: List[String],
                          columnValues: List[String] ) {

  // Map class for structured output stream
  case class MemMap(map: Map[String, String])

  private var memoryStreamMaps: MemoryStream[MemMap] = _
  private var readerSql: String = ""
  private var outputStream: StreamingQuery = _
  private var rddStream: DataFrame = _

  // Zip up the column names and values to create key value
  // Map with the column names being the key
  private val mapData = (columnNames zip columnValues) toMap
  private val schemaData = (columnNames zip columnTypes) toMap

  def primeRddStream(): Unit = {

    // Required to implicit to setup behind the scenes resolutions must
    // be defined before memoryStreamMaps and outputStream
    implicit val isc: SparkContext = {
      this.spark.sparkContext
    }
    import spark.implicits._
    implicit val sqlCtx: SQLContext = {
      this.spark.sqlContext
    }
    // Requires the above two line to resolve the Int encoder and SQL context
    // at runtime Always watchout for the needs of scala implecits
    // MemoryStream is an memory based stream avaiable in scala but not Java
    memoryStreamMaps = MemoryStream[MemMap]

    // Create structure of the in memory stream. Set is up as individual time windows that are 5 seconds in size and count the number of records recieved
    // inside of that time window if limit set
    val memStreamDF = this.memoryStreamMaps.toDF()

    // Create the stream and give it the name of the step. Make sure it has the compele output allows for intermediate processing. In the case of limit
    // Use OutputMode append and collect at the end.  Write to the stream based on a time trigger set for every 3 seconds. Must be close to but greater then
    // half the time window. In sures accurret spread of records if there gneration is uniform. Note MemoryStream is designed for testing and does not offer
    // full fault recovery.
    this.outputStream = memStreamDF.
      writeStream.
      format("memory").
      queryName(this.stepId).
      outputMode(OutputMode.Append).start()

    // Build select statement
    readerSql = "SELECT "
    schemaData foreach ( entry => readerSql += "cast(map['" + entry._1 + "'] as " + entry._2 + ") " + entry._1 + ", " )
    readerSql = readerSql.substring(0, readerSql.length - 2)
    readerSql += " FROM " + stepId
    // Initial RDD
    this.rddStream = spark.sql(readerSql)
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
    // Return copy not the orignal
    rddStream.toDF()
  }

  def waitOnStreamToTerminate(): Unit = {
    this.outputStream.awaitTermination()
  }

  def waitOnStreamToTerminate(seconds: Long): Unit = {
    this.outputStream.awaitTermination(seconds)
  }
}
