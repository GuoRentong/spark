// dog testing
package org.apache.spark.examples.oxygen

import java.lang.System.nanoTime
import java.io.File

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

// scalastyle:off println
object SpeedUp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("VectorAdd")
      .master("local[*]")
      .config("spark.driver.memory", "16g")
      .config("spark.sql.execution.arrow.enabled", "true")
      .config("parquetVectorizedReaderEnabled", "true")
      .config("spark.files.maxPartitionBytes", "1g")
      .config("", "")
      .getOrCreate()
    import spark.implicits._

    val FILENAME = "/home/mike/workspace/data/fucker2.parquet"
    val is_exist = new File(FILENAME).exists()

    if (!is_exist) {
      println("initializing file... shoule be slow...")
      val tmp_df = (0L until 16L * 1024L * 1024L).toDF("vals")
      tmp_df.write.parquet(FILENAME)
      println("initialized")
    }

    val idf = spark.read.parquet(FILENAME)

    idf.createOrReplaceTempView("data")

    val df = spark.sql("select vals + 1 from data")
    df.collect()

    val arr = df.collect()
    val sum = arr.map(_.get(0).asInstanceOf[Long]).sum
    println(s"sum=${sum}")

    spark.stop()
  }
}
// scalastyle:on println
