
package org.apache.spark.examples

import org.apache.spark.sql.catalyst.expressions.{Ascending, Descending, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Sort}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.{ExplainMode, SparkSession, SparkSessionExtensions}


case class ForbiddenDescSorts(spark: SparkSession) extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case ss@Sort(orders, isGlobalOrder, sortChild)
      if orders.size == 1 && orders(0).direction == Descending
    => orders(0) match {
      case SortOrder(orderChild, Descending, no, so)
      => Sort(Seq(SortOrder(orderChild, Ascending, no, so)), isGlobalOrder, sortChild)
    }
  }
}


// scalastyle:off println
object AggNaiveTest {
  def main(args: Array[String]): Unit = {
    type ExtensioniBuilder = SparkSessionExtensions => Unit

    val f: ExtensioniBuilder = _.injectOptimizerRule(ForbiddenDescSorts)

    val spark = SparkSession
      .builder
      .appName("AggNaive")
      .withExtensions(f)
      .getOrCreate()

    spark.udf.register("fuck", (s: Long) => s * s)

    import spark.implicits._

    val data = (0 until (1 * 1000 * 1000)).toList.toSeq.flatMap(iterIndex => Seq((iterIndex, 120)))

    val df = data
      .toDF("b", "c")

    df.createOrReplaceTempView("tt")
    val res = spark.sql("select fuck(fuck(b)) from tt");

    res.explain(ExplainMode.Extended)
    val dbgInfo = res.toString()
    println(dbgInfo)

    res.show()

    spark.stop()
  }
}

// scalastyle:on println
