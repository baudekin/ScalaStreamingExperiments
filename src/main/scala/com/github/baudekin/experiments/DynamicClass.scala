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

import scala.collection.mutable.{Map, HashMap}

import org.apache.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.execution.streaming.MemoryStream


object DynamicClass {

  case class MyMap(map: HashMap[String, String])

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("DynamicClass")
      .getOrCreate()

    // Required to implicit to setup behind the scenes resolutions
    implicit val isc = spark.sqlContext
    import spark.implicits._
    // Requires the above two line to resolve the Int encoder and SQL context
    // at runtime Always watchout for the needs of scala implecits
    // MemoryStream is an memory based stream avaiable in scala but not Java

    val rowsIn = MemoryStream[MyMap]
    val mymap = new HashMap[String, String]
    mymap.put("ColumnOne", "foobar")
    mymap.put("ColumnTwo", "1")
    mymap.put("ColumnThree", "10.99")
    val data = MyMap(mymap)
    rowsIn.addData(data)

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
      //option("path", "json").
      queryName("MemoryQuery").
      //  outputMode(OutputMode.Update).
      outputMode(OutputMode.Append).start()


    // Initial Record
    val stepOne = spark.sql("SELECT cast(Now as int) Now, map['ColumnOne'] as ColumnOne, cast(map['ColumnTwo'] as Integer) ColumnTwo, cast(map['ColumnThree'] as Double) ColumnThree FROM MemoryQuery")
    val orderByNow = stepOne.orderBy($"Now".desc)

    try {
      // Never stop Generating Data we should run out of memory at some point
      while (true) {
        try {
          Thread.sleep(5000)
        } catch {
          case iex: java.lang.InterruptedException => {
            Thread.currentThread().interrupt()
          }
        }
        outputStream.processAllAvailable()
        stepOne.show()
        val last = orderByNow.head(1)
        println("*****Last: ")
        println(last.toList(0))

        rowsIn.addData(data)
      } // End of Loop
    } catch {
      case tex: java.util.concurrent.TimeoutException => {
        tex.printStackTrace()
      }
      case iex: java.lang.InterruptedException => {
        iex.printStackTrace()
      }
      case e: Exception => e.printStackTrace()
      case r: RuntimeException => r.printStackTrace()
    } finally {
      outputStream.awaitTermination()
      println("Before Stream Stop")
      outputStream.stop()
      println("After Stream Stop")
      println("Before Spark Stop")
      spark.stop()
      println("After Spark Stop")
    }
  }
}
