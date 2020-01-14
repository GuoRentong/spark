/*
   dog testing
 */

package org.apache.spark.examples.oxygen

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{
  Add,
  Alias,
  AttributeReference,
  AttributeSeq,
  BoundReference,
  ExpectsInputTypes,
  ExprId,
  Expression,
  Literal,
  NamedExpression
}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.execution.{ColumnarRule, ProjectExec, RowToColumnarExec, SparkPlan}
import org.apache.spark.sql.types.{DataType, Decimal, LongType, Metadata}
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarArray, ColumnarBatch, ColumnarMap}
import org.apache.spark.unsafe.types.UTF8String

case class MyColumarRule(pre: Rule[SparkPlan], post: Rule[SparkPlan]) extends ColumnarRule {
  override def preColumnarTransitions: Rule[SparkPlan] = pre

  override def postColumnarTransitions: Rule[SparkPlan] = post
}

case class PreRuleReplaceAddWithBrokenVersion() extends Rule[SparkPlan] {
  def replaceWithColumnarExpression(exp: Expression): ColumnarExpression = exp match {
    case a: Alias =>
      new ColumnarAlias(replaceWithColumnarExpression(a.child), a.name)(
        a.exprId,
        a.qualifier,
        a.explicitMetadata)

    case att: AttributeReference =>
      new ColumnarAttributeReference(att.name, att.dataType, att.nullable, att.metadata)(
        att.exprId,
        att.qualifier)

    case lit: Literal =>
      new ColumnarLiteral(lit.value, lit.dataType)

    case add: Add
        if (add.dataType == LongType) &&
          (add.left.dataType == LongType) &&
          (add.right.dataType == LongType) =>
      // Add only supports Longs for now.
      new BrokenColumnarAdd(
        replaceWithColumnarExpression(add.left),
        replaceWithColumnarExpression(add.right))

    case inc: Inc if inc.dataType == LongType =>
      new Inc(inc.children.map(e => replaceWithColumnarExpression(e)))
    case exp =>
      throw new CannotReplaceException(
        s"expression " +
          s"${exp.getClass} ${exp} is not currently supported.")
  }

  def replaceWithColumnarPlan(plan: SparkPlan): SparkPlan =
    try plan match {
      case plan: ProjectExec =>
        new ColumnarProjectExec(
          plan.projectList.map((exp) =>
            replaceWithColumnarExpression(exp).asInstanceOf[NamedExpression]),
          replaceWithColumnarPlan(plan.child))
      case p =>
        logWarning(s"Columnar processing for ${p.getClass} is not currently supported.")
        p.withNewChildren(p.children.map(replaceWithColumnarPlan))
    } catch {
      case exp: CannotReplaceException =>
        logWarning(
          s"Columnar processing for ${plan.getClass} is not currently supported" +
            s"because ${exp.getMessage}")
        plan
    }

  override def apply(plan: SparkPlan): SparkPlan = replaceWithColumnarPlan(plan)
}

case class Inc(inputExpressions: Seq[Expression])
    extends ColumnarExpression
    with ExpectsInputTypes
    with CodegenFallback {
  assert(inputExpressions.length == 1)

  override def inputTypes = Seq(LongType)

  override def nullable = false

  override def eval(input: InternalRow) = new RuntimeException("Inc only call columnarEval")

  override def children: Seq[Expression] = inputExpressions

  override def supportsColumnar =
    inputExpressions(0).asInstanceOf[ColumnarExpression].supportsColumnar

  override def columnarEval(batch: ColumnarBatch): Any = {
    val input = inputExpressions(0).asInstanceOf[ColumnarExpression].columnarEval(batch)
    val result = new OnHeapColumnVector(batch.numRows(), dataType)
    println(s"in Inc ,num rows = ${batch.numRows()}")
    for (i <- 0 until batch.numRows()) {
      result.appendLong(input.asInstanceOf[ColumnVector].getLong(i) + 1)
    }
    result
  }

  override def dataType: DataType = inputExpressions(0).dataType
}

class BrokenColumnarAdd(left: ColumnarExpression, right: ColumnarExpression)
    extends Add(left, right)
    with ColumnarExpression {

  override def supportsColumnar(): Boolean = left.supportsColumnar && right.supportsColumnar

  override def columnarEval(batch: ColumnarBatch): Any = {
    var lhs: Any = null
    var rhs: Any = null
    var ret: Any = null
    try {
      lhs = left.columnarEval(batch)
      rhs = right.columnarEval(batch)

      if (lhs == null || rhs == null) ret = null
      else if (lhs.isInstanceOf[ColumnVector] && rhs.isInstanceOf[ColumnVector]) {
        val l = lhs.asInstanceOf[ColumnVector]
        val r = rhs.asInstanceOf[ColumnVector]
        val result = new OnHeapColumnVector(batch.numRows(), dataType)
        ret = result

        for (i <- 0 until batch.numRows()) {
          result.appendLong(l.getLong(i) + r.getLong(i)) // BUG to show we replaced Add
        }
      } else if (rhs.isInstanceOf[ColumnVector]) {
        val l = lhs.asInstanceOf[Long]
        val r = rhs.asInstanceOf[ColumnVector]
        val result = new OnHeapColumnVector(batch.numRows(), dataType)
        ret = result

        for (i <- 0 until batch.numRows()) {
          result.appendLong(l + r.getLong(i)) // BUG to show we replaced Add
        }
      } else if (lhs.isInstanceOf[ColumnVector]) {
        val l = lhs.asInstanceOf[ColumnVector]
        val r = rhs.asInstanceOf[Long]
        val result = new OnHeapColumnVector(batch.numRows(), dataType)
        ret = result

        println(s"left is vector,num rows = ${batch.numRows()}")

        for (i <- 0 until batch.numRows()) {
          result.appendLong(l.getLong(i) + r) // BUG to show we replaced Add
        }
      } else ret = nullSafeEval(lhs, rhs)
    } finally {
      if (lhs != null && lhs.isInstanceOf[ColumnVector]) lhs.asInstanceOf[ColumnVector].close()
      if (rhs != null && rhs.isInstanceOf[ColumnVector]) rhs.asInstanceOf[ColumnVector].close()
    }
    ret
  }
}

trait ColumnarExpression extends Expression with Serializable {
  def supportsColumnar = true

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

class ColumnarAlias(child: ColumnarExpression, name: String)(
    override val exprId: ExprId = NamedExpression.newExprId,
    override val qualifier: Seq[String] = Seq.empty,
    override val explicitMetadata: Option[Metadata] = None)
    extends Alias(child, name)(exprId, qualifier, explicitMetadata)
    with ColumnarExpression {

  override def columnarEval(batch: ColumnarBatch): Any = child.columnarEval(batch)
}

class ColumnarAttributeReference(
    name: String,
    dataType: DataType,
    nullable: Boolean = true,
    override val metadata: Metadata = Metadata.empty)(
    override val exprId: ExprId = NamedExpression.newExprId,
    override val qualifier: Seq[String] = Seq.empty[String])
    extends AttributeReference(name, dataType, nullable, metadata)(exprId, qualifier)
    with ColumnarExpression {

  // No columnar eval is needed because this must be bound before it is evaluated
}

class ColumnarLiteral(value: Any, dataType: DataType)
    extends Literal(value, dataType)
    with ColumnarExpression {
  override def columnarEval(batch: ColumnarBatch): Any = value
}

class CannotReplaceException(str: String) extends RuntimeException(str) {}

class ColumnarProjectExec(projectList: Seq[NamedExpression], child: SparkPlan)
    extends ProjectExec(projectList, child) {

  // Disable code generation
  override def supportCodegen: Boolean = supportsColumnar == false

  override def supportsColumnar: Boolean =
    projectList.forall(_.asInstanceOf[ColumnarExpression].supportsColumnar)

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val boundProjectList =
      ColumnarBindReferences.bindReferences(
        projectList.asInstanceOf[Seq[ColumnarExpression]],
        child.output)

    val rdd = child.executeColumnar()
    rdd.mapPartitions(
      (itr) =>
        CloseableColumnBatchIterator(
          itr,
          (cb) => {
            val newColumns = boundProjectList
              .map(
                expr =>
                  expr
                    .asInstanceOf[ColumnarExpression]
                    .columnarEval(cb)
                    .asInstanceOf[ColumnVector])
              .toArray
            new ColumnarBatch(newColumns, cb.numRows())
          }))
  }

  // We have to override equals because subclassing a case class like ProjectExec is not that clean
  // One of the issues is that the generated equals will see ColumnarProjectExec and ProjectExec
  // as being equal and this can result in the withNewChildren method not actually replacing
  // anything
  override def equals(other: Any): Boolean = {
    if (!super.equals(other)) return false
    other.isInstanceOf[ColumnarProjectExec]
  }

  override def hashCode(): Int = super.hashCode
}

object ColumnarBindReferences extends Logging {

  /**
   * A helper function to bind given expressions to an input schema.
   */
  def bindReferences[A <: ColumnarExpression](expressions: Seq[A], input: AttributeSeq): Seq[A] =
    expressions.map(ColumnarBindReferences.bindReference(_, input))

  // Mostly copied from BoundAttribute.scala so we can do columnar processing
  def bindReference[A <: ColumnarExpression](
      expression: A,
      input: AttributeSeq,
      allowFailures: Boolean = false): A =
    expression
      .transform {
        case a: AttributeReference =>
          val ordinal = input.indexOf(a.exprId)
          if (ordinal == -1)
            if (allowFailures) a
            else sys.error(s"Couldn't find $a in ${input.attrs.mkString("[", ",", "]")}")
          else new ColumnarBoundReference(ordinal, a.dataType, input(ordinal).nullable)
      }
      .asInstanceOf[A]
}

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

  override def next(): ColumnarBatch = {
    closeCurrentBatch()
    cb = f(itr.next())
    cb
  }

  private def closeCurrentBatch(): Unit = {
    if (cb != null) {
      cb.close
      cb = null
    }
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
   * Don't actually close the ColumnVector this wraps.  The producer of the vector will take
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
