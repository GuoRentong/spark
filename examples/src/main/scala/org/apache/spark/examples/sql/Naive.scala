
package org.apache.spark.examples.sql

// $example on:programmatic_schema$
// $example off:programmatic_schema$
// $example on:init_session$
import org.apache.spark.sql.catalyst.analysis.UnresolvedStar
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, Multiply}
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}

// $example off:init_session$
// $example on:programmatic_schema$
// $example on:data_types$
import org.apache.spark.sql.types._
// $example off:data_types$
// $example off:programmatic_schema$


object Naive {

  // an example for optimizer rule customize
  case class MultiplyOptimizationRule(spark: SparkSession) extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
      case mul@Multiply(left, right) if right.isInstanceOf[Literal] &&
        right.asInstanceOf[Literal].value.asInstanceOf[Long] == 3 =>
        left
    }
  }

  // an example for parser rule customize
  class StrictParser(parser: ParserInterface) extends ParserInterface {
    /**
     * Parse a string to a [[LogicalPlan]].
     */
    override def parsePlan(sqlText: String): LogicalPlan = {
      val logicalPlan = parser.parsePlan(sqlText)
      logicalPlan transform {
        case project@Project(projectList, _) =>
          projectList.foreach {
            name =>
              if (name.isInstanceOf[UnresolvedStar]) {
                throw new RuntimeException("********This parser rule is defined by czp.*********\n")
              } // Triggered when there is a project `select *`
          }
          project
      }
      logicalPlan
    }

    /**
     * Parse a string to an [[Expression]].
     */
    override def parseExpression(sqlText: String): Expression = parser.parseExpression(sqlText)

    /**
     * Parse a string to a [[TableIdentifier]].
     */
    override def parseTableIdentifier(sqlText: String): TableIdentifier =
      parser.parseTableIdentifier(sqlText)

    /**
     * Parse a string to a [[FunctionIdentifier]].
     */
    override def parseFunctionIdentifier(sqlText: String): FunctionIdentifier =
      parser.parseFunctionIdentifier(sqlText)

    /**
     * Parse a string to a [[StructType]]. The passed SQL string should be a comma separated
     * list of field definitions which will preserve the correct Hive metadata.
     */
    override def parseTableSchema(sqlText: String): StructType =
      parser.parseTableSchema(sqlText)

    /**
     * Parse a string to a [[DataType]].
     */
    override def parseDataType(sqlText: String): DataType = parser.parseDataType(sqlText)

    def parseMultipartIdentifier(sqlText: String): Seq[String] =
      parser.parseMultipartIdentifier(sqlText)
  }

  def main(args: Array[String]): Unit = {

    type ParserBuilder = (SparkSession, ParserInterface) => ParserInterface
    type ExtensionsBuilder = SparkSessionExtensions => Unit

    // $example on:init_session$
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .withExtensions(e => e.injectParser((_, parser) => new StrictParser(parser)) )
      .getOrCreate()

    val df = spark.read.json("/home/mike/workspace/spark-research/spark/examples/src/main/resources/people.json")
    df.createOrReplaceTempView("people")
    val sqlDF2 = spark.sql("SELECT * FROM people")

    spark.stop

  }
}
