// dog testing
package org.apache.spark.examples.oxygen.demo

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{
  Alias,
  AttributeReference,
  AttributeSeq,
  BoundReference,
  ExprId,
  Literal,
  NamedExpression
}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ProjectExec, RowToColumnarExec, SparkPlan}
import org.apache.spark.sql.types.{DataType, Decimal, Metadata}
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarArray, ColumnarBatch, ColumnarMap}
import org.apache.spark.unsafe.types.UTF8String

class FrameWork {}

class CannotReplaceException(str: String) extends RuntimeException(str) {}

case class CloseableColumnBatchIterator(
    itr: Iterator[ColumnarBatch],
    f: ColumnarBatch => ColumnarBatch)
    extends Iterator[ColumnarBatch] {
  var cb: ColumnarBatch = null

  override def hasNext: Boolean = {
    closeCurrentBatch()
    itr.hasNext
  }

  TaskContext
    .get()
    .addTaskCompletionListener[Unit]((tc: TaskContext) => {
      closeCurrentBatch()
    })

  private def closeCurrentBatch(): Unit = {
    if (cb != null) {
      cb.close
      cb = null
    }
  }

  override def next(): ColumnarBatch = {
    closeCurrentBatch()
    cb = f(itr.next())
    cb
  }
}

class ColumnarBoundReference(ordinal: Int, dataType: DataType, nullable: Boolean)
    extends BoundReference(ordinal, dataType, nullable)
    with ColumnarExpression {

  override def columnarEval(batch: ColumnarBatch): Any = {
    // Because of the convention that the returned ColumnVector must be closed by the
    // caller we wrap this column vector so a close is a NOOP, and let the original source
    // of the vector close it.
    NoCloseColumnVector.wrapIfNeeded(batch.column(ordinal))
  }
}

object NoCloseColumnVector extends Logging {
  def wrapIfNeeded(cv: ColumnVector): NoCloseColumnVector = cv match {
    case ref: NoCloseColumnVector =>
      ref
    case vec => NoCloseColumnVector(vec)
  }
}

case class NoCloseColumnVector(wrapped: ColumnVector) extends ColumnVector(wrapped.dataType) {
  private var refCount = 1

  /**
   * Don't actually close the ColumnVector this wraps. The producer of the vector will take
   * care of that.
   */
  override def close(): Unit = {
    // Empty
  }

  override def hasNull: Boolean = wrapped.hasNull

  override def numNulls(): Int = wrapped.numNulls

  override def isNullAt(rowId: Int): Boolean = wrapped.isNullAt(rowId)

  override def getBoolean(rowId: Int): Boolean = wrapped.getBoolean(rowId)

  override def getByte(rowId: Int): Byte = wrapped.getByte(rowId)

  override def getShort(rowId: Int): Short = wrapped.getShort(rowId)

  override def getInt(rowId: Int): Int = wrapped.getInt(rowId)

  override def getLong(rowId: Int): Long = wrapped.getLong(rowId)

  override def getFloat(rowId: Int): Float = wrapped.getFloat(rowId)

  override def getDouble(rowId: Int): Double = wrapped.getDouble(rowId)

  override def getArray(rowId: Int): ColumnarArray = wrapped.getArray(rowId)

  override def getMap(ordinal: Int): ColumnarMap = wrapped.getMap(ordinal)

  override def getDecimal(rowId: Int, precision: Int, scale: Int): Decimal =
    wrapped.getDecimal(rowId, precision, scale)

  override def getUTF8String(rowId: Int): UTF8String = wrapped.getUTF8String(rowId)

  override def getBinary(rowId: Int): Array[Byte] = wrapped.getBinary(rowId)

  override protected def getChild(ordinal: Int): ColumnVector = wrapped.getChild(ordinal)
}

case class MyPostRule() extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = plan match {
    case rc: RowToColumnarExec => new ReplacedRowToColumnarExec(rc.child)
    case plan => plan.withNewChildren(plan.children.map(apply))
  }
}

class ReplacedRowToColumnarExec(override val child: SparkPlan) extends RowToColumnarExec(child) {

  // We have to override equals because subclassing a case class like ProjectExec is not that clean
  // One of the issues is that the generated equals will see ColumnarProjectExec and ProjectExec
  // as being equal and this can result in the withNewChildren method not actually replacing
  // anything
  override def equals(other: Any): Boolean = {
    if (!super.equals(other)) return false
    other.isInstanceOf[ReplacedRowToColumnarExec]
  }

  override def hashCode(): Int = super.hashCode()
}
