// dog testing
package org.apache.spark.examples.oxygen.demo

import java.lang.System.nanoTime

import org.apache.log4j.{Level, Logger}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession;

// scalastyle:off println
object IncTest extends Logging {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("VectorAdd")
      .master("local[*]")
      .config("spark.driver.memory", "16g")
      .config("spark.sql.execution.arrow.enabled", "true")
      .config("parquetVectorizedReaderEnabled", "true")
      .config("spark.files.maxPartitionBytes", "1g")
      .withExtensions { extensions =>
        extensions.injectColumnar(session =>
          MyColumarRule(PreRuleReplaceAddWithBrokenVersion(), MyPostRule()))
      }
      .getOrCreate()

    spark.sessionState.functionRegistry.createOrReplaceTempFunction("inc", Inc)
    Logger.getLogger("org").setLevel(Level.WARN)

    assert(
      spark.sessionState.columnarRules
        .contains(MyColumarRule(PreRuleReplaceAddWithBrokenVersion(), MyPostRule())))

    val t0 = logger("worker begin")

    val t1 = logger("input ready")
    val idf = spark.read.parquet("/home/mike/workspace/data/fucker.parquet")

    val t2 = logger("idf ready")
    idf.createTempView("data")

    val t3 = logger("register view ready")

    val df = spark.sql("select vals + 1 from data")
    df.collect()

    val t4 = logger("selector to array ready")

//    val df2 = spark.sql("select inc(vals) as res from data")
//    df2.collect()

    val arr = df.collect()
    val sum = arr.map(_.get(0).asInstanceOf[Long]).sum
    println(s"sum=${sum}")
    val t5 = logger("summer")

    spark.stop()
  }

  val init_time = nanoTime().toDouble * 1e-9
  var last_time = init_time
  def logger(msg: String): Double = {
    val time = nanoTime().toDouble * 1e-9
    println(s"${msg} @delta=${time - last_time} total=${time - init_time}")
    last_time = time
    time
  }
}
