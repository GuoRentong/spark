// dog testing
package org.apache.spark.examples.oxygen

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{
  Attribute,
  AttributeSet,
  ExprId,
  Expression,
  NamedExpression,
  NonSQLExpression,
  Unevaluable,
  UserDefinedExpression
}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryNode}
import org.apache.spark.sql.types.{DataType, LongType}

// should follow PythonUDF ways

object OxygenUDF {
  def isScalarOxygenUDF(e: Expression): Boolean = {
    e.isInstanceOf[OxygenUDF]
  }

  def register(spark: SparkSession): Unit = {
    // TODO: add function here
    val udf = UserDefinedOxygenFunction(
      "inc",
      new OxygenFunction("inc"),
      LongType,
      udfDeterministic = true)
    spark.sessionState.functionRegistry.createOrReplaceTempFunction("inc", udf.builder)
  }

}

// this is OxygenUDF builder
case class UserDefinedOxygenFunction(
    name: String,
    func: OxygenFunction,
    dataType: DataType,
    udfDeterministic: Boolean) {
  def builder(children: Seq[Expression]): Expression = {
    OxygenUDF(name, func, dataType, children, udfDeterministic)
  }
}

class OxygenFunction(name: String) extends Serializable {
  // TODO: add new methods to use
}

private[oxygen] case class OxygenUDF(
    name: String,
    func: OxygenFunction,
    dataType: DataType,
    children: Seq[Expression],
    udfDeterministic: Boolean,
    resultId: ExprId = NamedExpression.newExprId)
    extends Expression
    with Unevaluable
    with NonSQLExpression
    with UserDefinedExpression {

  override lazy val deterministic: Boolean = udfDeterministic && children.forall(_.deterministic)
  override lazy val canonicalized: Expression = {
    val canoicalizedChildren = children.map(_.canonicalized)
    this.copy(resultId = ExprId(-1)).withNewChildren(canoicalizedChildren)
  }

  override def toString: String = s"${name}(${children.mkString(", ")})"

  override def nullable: Boolean = true
}

trait BaseEvalOxygen extends UnaryNode {
  def udfs: Seq[OxygenUDF]
  def resultAttrs: Seq[Attribute]
  override def output: Seq[Attribute] = child.output ++ resultAttrs
  override def producedAttributes: AttributeSet = AttributeSet(resultAttrs)
}

case class ArrowEvalOxygen(udfs: Seq[OxygenUDF], resultAttrs: Seq[Attribute], child: LogicalPlan)
    extends BaseEvalOxygen
