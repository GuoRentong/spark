
package org.apache.spark.examples

import org.apache.spark.sql.catalyst.expressions.{Descending, SortOrder, Ascending}
import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.{DataFrame, ExplainMode, Row, SQLContext, SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Sort}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.zookeeper.KeeperException.UnimplementedException


case class ForbiddenDescSorts(spark: SparkSession) extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case ss@Sort(orders, isGlobalOrder, sortChild) if orders.size == 1 && orders(0).direction == Descending
    => orders(0) match {
      case SortOrder(orderChild, Descending, no, so)
      => Sort(Seq(SortOrder(orderChild, Ascending, no, so)), isGlobalOrder, sortChild)
    }
  }
}

//case class FuckParser(spark: SparkSession) extends

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

    import spark.implicits._

    val data = Seq(
      ("a", 1, 1),
      ("b", 1, 2),
      ("c", 3, 3),
      ("d", 3, 4),
      ("a", 1 + 5, 11),
      ("b", 1 + 5, 12),
      ("c", 3 + 5, 13),
      ("d", 3 + 5, 14),
      ("a", 2, 5),
      ("b", 2, 6),
      ("c", 4, 7),
      ("d", 4, 8),
      ("a", 2 + 5, 15),
      ("b", 2 + 5, 16),
      ("c", 4 + 5, 17),
      ("d", 4 + 5, 18),
    )

    val df = data
      .toDF("a", "b", "c")

    val res = df
      .select('a, 'b, 'c)
      .orderBy('c.desc)

    res.explain(ExplainMode.Extended)
    val dbgInfo = res.toString()
    println(dbgInfo)

    res.show()

    spark.stop()
  }
}

// scalastyle:on println
