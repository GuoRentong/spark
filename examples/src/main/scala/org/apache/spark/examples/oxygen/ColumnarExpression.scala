// fucker
package org.apache.spark.examples.oxygen

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.vectorized.ColumnarBatch

trait ColumnarExpression extends Expression with Serializable {
  def supportsColumnar: Boolean = true

  def columnarEval(batch: ColumnarBatch): Any =
    throw new IllegalStateException(
      s"Internal Error ${this.getClass} has column support mismatch")

  // We need to override equals because we are subclassing a case class
  override def equals(other: Any): Boolean = {
    if (!super.equals(other)) return false
    other.isInstanceOf[ColumnarExpression]
  }

  override def hashCode(): Int = super.hashCode()
}
