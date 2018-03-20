/*
* The MIT License (MIT)
*
* Copyright (c) 2002 - 2017, Hitachi Vantara
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

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

import scala.concurrent.duration._

object StructuredStreamingStepExample {

  // This based off of Jace Klaskowsiki's example see https://github.com/jaceklaskowski/spark-structured-streaming-book/blob/master/spark-sql-streaming-MemoryStream.adoc
  // The Spark 2.2.0 Structured Streaming Programming Guide is located here: https://spark.apache.org/docs/2.2.0/structured-streaming-programming-guide.html
  def main(args: Array[String]): Unit = {
    // You need atleast two threads to use the streaming API
    val spark = SparkSession.builder
      .master("local[2]")
      .appName("StructuredStreamingStepExample")
      .getOrCreate()

    // Required to implicit to setup behind the scenes resolutions
    implicit val isc = spark.sqlContext
    import spark.implicits._
    // Requires the above two line to resolve the Int encoder and SQL context
    // at runtime Always watchout for the needs of scala implecits
    // MemoryStream is an memory based stream avaiable in scala but not Java
    val intsIn = MemoryStream[Int]

    // Create structure of the in memory stream. Set is up as individual time windows that are 5 seconds in size and count the number of records recieved
    // inside of that time window
    val schema = intsIn.toDF.withColumn("now", current_timestamp()).withWatermark("now", "5 seconds").groupBy(window($"now", "5 seconds") as "window").agg(count("*") as "total")

    // Create the stream and give it the name "MemoryQuery". Make sure it has the compele output allows for intermediate processing. In the case of limit
    // Use OutputMode append and collect at the end.  Write to the stream based on a time trigger set for every 3 seconds. Must be close to but greater then
    // half the time window. In sures accurret spread of records if there gneration is uniform. Note MemoryStream is designed for testing and does not offer
    // full fault recovery.
    val outputStream = schema.
      writeStream.
      format("memory").
//      format("json").
      option("path", "json").
      queryName("MemoryQuery").
      outputMode(OutputMode.Update).
//      outputMode(OutputMode.Append).
      trigger(Trigger.ProcessingTime(3.seconds)).
      start

    println("#######:isActive:" + outputStream.isActive )
    println("#######:status..:" + outputStream.status )

    // Add data to the in memory stream
    val zeroOffset = intsIn.addData(0, 1, 2)
    outputStream.processAllAvailable()
    println("#######:lastProgress:")
    outputStream.lastProgress.sources foreach println
    println("#######:MemoryQuerycollect:")
    spark.table("MemoryQuery").collect() foreach println

    // New step
    val step = spark.table("MemoryQuery").selectExpr("window", "total").toDF()
    println("#######:step.show:")
    step.show()

    println("#######:addData:")
    val oneOffset = intsIn.addData(3, 4, 5, 6, 7)
    println("#######:processAllAvaialable:")
    outputStream.processAllAvailable()
    println("#######:lastProgress:")
    outputStream.lastProgress.sources foreach println
    println("#######:toDF:")
    // Get the data frame and perform query on it
    val df = spark.table("MemoryQuery").toDF()
    println("#######:select:")
    val sumCounts = df.select(col("total")).rdd.map(_(0).asInstanceOf[Long]).reduce(_+_)
    println("#######:sumCounts:" + sumCounts)
    println("#######:MemoryQuerycollect:")
    spark.table("MemoryQuery").collect() foreach println
    println("#######:step.show:")
    step.show()
    step.toDF().write.mode(SaveMode.Overwrite).json("/user/mbodkin/fileoutstep")

    println("#######:addData:")
    val twoOffset = intsIn.addData(8, 9, 10, 11, 12, 13, 14, 15)
    println("#######:processAllAvaialable:")
    outputStream.processAllAvailable()
    println("#######:lastProgress:")
    outputStream.lastProgress.sources foreach println
    // Get the data frame and perform query on it
    println("#######:toDF:")
    val df2 = spark.table("MemoryQuery").toDF()
    println("#######:select:")
    val sumCounts2 = df2.select(col("total")).rdd.map(_(0).asInstanceOf[Long]).reduce(_+_)
    println("#######:sumCounts2:" + sumCounts2)
    println(sumCounts2)
    println("#######:df2.collect:")
    df2.collect() foreach println

    // Wait for the collect to finish and then stop the stream
    Thread.sleep(1000)
    //step.toDF().write.mode(SaveMode.Overwrite).json("/user/mbodkin/fileoutstep")
    outputStream.stop()
  }
}
