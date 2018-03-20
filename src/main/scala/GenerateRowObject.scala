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
package com.github.baudekin

import org.apache.spark
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

import scala.concurrent.duration._

import java.util.Calendar

object GenerateRowObject {

  case class GenResults(ColumnOne: String, ColumnTwo: Integer, ColumnThree: Double, Current: Long, LastTime: Long)
  private var _lastTimeStamp: Long = 0

  private def createData: GenResults = {
    val now = Calendar.getInstance.getTimeInMillis
    val row = GenResults("ValueOne", 10, 10.99, now, _lastTimeStamp)
    _lastTimeStamp = now
    return row
  }

  private def startStream(spark: SparkSession): Unit = {
    // Required to implicit to setup behind the scenes resolutions
    implicit val isc = spark.sqlContext
    import spark.implicits._
    // Requires the above two line to resolve the Int encoder and SQL context
    // at runtime Always watchout for the needs of scala implecits
    // MemoryStream is an memory based stream avaiable in scala but not Java
    import org.apache.spark.sql.Encoders
    val schema = Encoders.product[GenResults].schema

    val rowsIn = MemoryStream[GenResults]
    rowsIn.addData(createData)

    // Create structure of the in memory stream. Set is up as individual time windows that are 5 seconds in size and count the number of records recieved
    // inside of that time window
    val inDF = rowsIn.toDF.withColumn("Now", current_timestamp())

    // Create the stream and give it the name "MemoryQuery". Make sure it has the compele output allows for intermediate processing. In the case of limit
    // Use OutputMode append and collect at the end.  Write to the stream based on a time trigger set for every 3 seconds. Must be close to but greater then
    // half the time window. In sures accurret spread of records if there gneration is uniform. Note MemoryStream is designed for testing and does not offer
    // full fault recovery.
    val outputStream = inDF.
      writeStream.
      format("memory").
      option("path", "json").
      queryName("MemoryQuery").
      //  outputMode(OutputMode.Update).
      outputMode(OutputMode.Append).
      start

    // Simulate Step of Doubling of Column Three
    val stepOne = spark.table("MemoryQuery").selectExpr("ColumnOne", "ColumnTwo", "ColumnThree", "ColumnTwo * 2 ColumnFour", "Current", "LastTime",  "Now").toDF()
    // Simulate Step of Doubling of ColumnFour
    val stepTwo = stepOne.selectExpr("ColumnOne", "ColumnTwo", "ColumnThree", "ColumnFour", "ColumnFour * 2 ColumnFive", "Current", "LastTime",  "Now").toDF()
    // Simulating Always getting the latest row note this is nevers updates it is always initialized to the first row because we
    // terminate it with parallelize
    val stepThree = spark.sparkContext.parallelize(
      Seq(
        stepTwo.reduce {
          (x, y) => if (x.getAs[Int]("Now") > y.getAs[Int]("Now")) x else y
        }
      )
    ).toJavaRDD()

    // Proper way is to order the RDD then when we want the last row we pull it off
    val latest = stepTwo.orderBy($"Now".desc)

    // Never stop Generating Data we should run out of memory at some point
    while (true) {
      Thread.sleep(5000)
      outputStream.processAllAvailable()
      println("#######")
      println("####### Process Row")
      println("#######")
      spark.table("MemoryQuery").collect() foreach println
      println("####### Step Two")
      // false tells show not to truncation columns
      stepTwo.show(false)
      println("####### Step Three:")
      stepThree.rdd.foreach( r => { println(r) })
      println("####### Latest:")
      // Show only first row and don't truncate it
      latest.show(1, false)
      rowsIn.addData(createData)
    }
    outputStream.stop()

  }

}
