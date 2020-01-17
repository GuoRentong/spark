/*
   dog testing
 */

package org.apache.spark.examples.oxygen.demo

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
