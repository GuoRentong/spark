// dog testing
package org.apache.spark.examples.oxygen

import java.lang.System.nanoTime

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

// scalastyle:off println
object Oxygen {
  val init_time = nanoTime().toDouble * 1e-9
  var last_time = init_time

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("VectorAdd")
      .master("local[*]")
      .config("spark.driver.memory", "16g")
      .config("spark.sql.execution.arrow.enabled", "true")
      .config("parquetVectorizedReaderEnabled", "true")
      .config("spark.files.maxPartitionBytes", "1g")
//      .withExtensions { extensions =>
//        extensions.injectColumnar(session =>
//          MyColumarRule(PreRuleReplaceAddWithBrokenVersion(), MyPostRule()))
//      }
      .getOrCreate()

//    spark.sessionState.functionRegistry.createOrReplaceTempFunction("inc", Inc)

    Logger.getLogger("org").setLevel(Level.WARN)
//    assert(
//      spark.sessionState.columnarRules
//        .contains(MyColumarRule(PreRuleReplaceAddWithBrokenVersion(), MyPostRule())))
    val t0 = logger("worker begin")
    val input: Seq[Long] = (0L until 1024L * 1024L * 4).toArray.toSeq
//    val input: Seq[Long] = Seq(0L, 1024L)
    import spark.implicits._

    val t1 = logger("input ready")
    val idf = input.toDF("vals").repartition(100)

    val t2 = logger("idf ready")
    println(idf.rdd.getNumPartitions)

    idf.write.parquet("/home/mike/workspace/data/fucker.parquet"

    idf.createOrReplaceTempView("data")
    val t3 = logger("register view ready")

    val df = spark.sql("select vals + 1 from data")
    df.collect()
    val t4 = logger("selector to array ready")

//    val df2 = spark.sql("select inc(vals) as res from data")
//
//    df2.collect().foreach(println)
    val arr = df.collect()
    val sum = arr.map(_.get(0).asInstanceOf[Long]).sum
    println(s"sum=${sum}")
    val t5 = logger("summer")
    //    val df = data.selectExpr("vals + 1")
    //    df.collect().foreach(println)

    spark.stop()
  }

  def logger(msg: String): Double = {
    val time = nanoTime().toDouble * 1e-9
    println(s"${msg} @delta=${time - last_time} total=${time - init_time}")
    last_time = time
    time
  }
}
// scalastyle:on println
