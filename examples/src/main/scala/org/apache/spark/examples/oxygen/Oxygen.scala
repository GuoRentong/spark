// dog testing
package org.apache.spark.examples.oxygen

import org.apache.log4j.{Level, Logger}

import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.internal.SQLConf.COLUMN_BATCH_SIZE

// scalastyle:off println
object Oxygen {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("VectorAdd")
      .master("local[*]")
      .config(COLUMN_BATCH_SIZE.key, 3)
      .config("parquetVectorizedReaderEnabled", true)
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

    val input = Seq(100L, 200L, 300L)
    import spark.implicits._

    input.toDF("vals").repartition(1).createOrReplaceTempView("data")

    val df = spark.sql("select vals + 2, vals + 1 , vals + 100 from data")
    df.collect().foreach(println)

    val df2 = spark.sql("select sum(inc(vals)) from data")
    df2.collect().foreach(println)

    //    val df = data.selectExpr("vals + 1")
    //    df.collect().foreach(println)

    spark.stop()
  }
}
// scalastyle:on println
