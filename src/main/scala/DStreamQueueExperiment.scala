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

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

// This is based off Spark Streaming Programming Guide: https://spark.apache.org/docs/2.2.0/streaming-programming-guide.html
object DStreamQueueExperiment {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("DStreamQueueExperiment").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(10))

    val rddQ = new mutable.Queue[RDD[Int]]()

    val intStream = ssc.queueStream(rddQ)
    val map = intStream.map(x => (x * x, 1))
    val reduce = map.reduceByKey( (x1 , x2) => x1 + x2 )


    reduce.print()
    ssc.start()

    // Create RDDs
    /*
    for (i <- 1 to 5) {
      rddQ.synchronized {
        rddQ += ssc.sparkContext.makeRDD(1 to 1000,  10)
      }
      Thread.sleep(1000)
    }
    */

    // Note never terminates until kill signal is recieved by the spark process
    while (true) {
        rddQ.synchronized {
          rddQ += ssc.sparkContext.makeRDD(1 to 1000,  10)
        }
      Thread.sleep(1000)
    }

    ssc.stop()
  }

}
