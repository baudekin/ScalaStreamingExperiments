package com.github.baudekin.experiments

import java.io.{OutputStreamWriter, PrintWriter}
import java.net.ServerSocket

import org.apache.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.OutputMode
import java.util.Calendar


object BasicSocketReader {
  class MyProducer extends Runnable {
    private var port:Int = 2112

    def run: Unit = {
      val ss = new ServerSocket(this.port)
      val soc = ss.accept()
      val out: PrintWriter = new PrintWriter(new OutputStreamWriter(soc.getOutputStream))
      while (true) {
        try  {
          Thread.sleep(5000)
        } catch {
          case iex: java.lang.InterruptedException => {
            Thread.currentThread().interrupt()
          }
          case tex: java.util.concurrent.TimeoutException => {
            tex.printStackTrace()
          }
        }
        out.println("Foobar " + Calendar.getInstance().getTime)
        out.flush()
      }
    }
  }

  def createStreamSocketStream(spark: SparkSession, port: Int, duration: Int): Unit = {

    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", port)
      .load

    // Start Generating Rows
    val p = new MyProducer
    val t = new Thread(p)
    t.start

    // Define steps
    import spark.implicits._
    val words = lines
      .as[String]
      .flatMap(_.split("\\s+"))

    import spark.implicits._
    val wordCounts = words
      .groupByKey(_.toLowerCase)
      .count()
      .orderBy($"count(1)" desc)

    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    // You need atleast two threads to use the streaming API
    val spark = SparkSession.builder
      .appName("StructuredStreamingStepExample")
      .getOrCreate()

    val port = Integer.valueOf(args(0))

    println("Started on port: " + port)
    println("Started Socket Steam" + Calendar.getInstance().getTime())
    createStreamSocketStream(spark, port, 600)
    println("Ended Socket Steam" + Calendar.getInstance().getTime())
  }
}
