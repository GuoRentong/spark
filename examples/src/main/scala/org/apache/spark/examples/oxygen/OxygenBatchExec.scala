// dog testing
package org.apache.spark.examples.oxygen

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryNode}
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}

object OxygenBatchExec {}

case class ArrowEvalOxygen(udfs: Seq[OxygenUDF], resultAttrs: Seq[Attribute], child: LogicalPlan)
    extends BaseEvalOxygen

case class EvalOxygenExec(udfs: Seq[OxygenUDF], resultAttrs: Seq[Attribute], child: SparkPlan)
    extends UnaryExecNode {
  override def output: Seq[Attribute] = child.output ++ resultAttrs

  override def producedAttributes: AttributeSet = AttributeSet(resultAttrs)

  private def doExecute(): RDD[InternalRow] = throw Exception("fucking dog")
}

trait BaseEvalOxygen extends UnaryNode {
  def udfs: Seq[OxygenUDF]
  def resultAttrs: Seq[Attribute]
  override def output: Seq[Attribute] = child.output ++ resultAttrs
  override def producedAttributes: AttributeSet = AttributeSet(resultAttrs)
}
