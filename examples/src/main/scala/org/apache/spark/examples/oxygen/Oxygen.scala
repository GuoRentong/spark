// dog testing
package org.apache.spark.examples.oxygen
import java.io.File

import org.apache.log4j.{Level, Logger}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

// scalastyle:off println
object Oxygen extends Logging {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)

    val spark = SparkSession
      .builder()
      .appName("VectorAdd")
      .master("local[*]")
      .config("spark.driver.memory", "16g")
      .config("spark.sql.execution.arrow.enabled", "true")
      .config("parquetVectorizedReaderEnabled", "true")
      .config("spark.files.maxPartitionBytes", "1g")
      .getOrCreate()
    import spark.implicits._

    OxygenUDF.register(spark)
    spark.experimental.extraOptimizations = Oxygen.log.warn("fuck")

    val FILENAME = "/home/mike/workspace/data/fucker2.parquet"
    val is_exist = new File(FILENAME).exists()

    log.warn("fuck")
    if (!is_exist) {
      println("initializing file... shoule be slow...")
      val tmp_df = (0L until 16L * 1024L * 1024L).toDF("vals")
      tmp_df.write.parquet(FILENAME)
      println("initialized")
    }

    log.warn("fuck")
    val idf = spark.read.parquet(FILENAME)

    log.warn("fuck")
    idf.createOrReplaceTempView("data")

    log.warn("fuck")
    val df = spark.sql("select inc(vals) from data")
    df.collect()

    val arr = df.collect()
    val sum = arr.map(_.get(0).asInstanceOf[Long]).sum
    println(s"sum=${sum}")

    spark.stop()
  }
}
// scalastyle:on println
