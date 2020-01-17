// dog testing
package org.apache.spark.examples.oxygen.demo

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{
  Alias,
  AttributeReference,
  AttributeSeq,
  ExprId,
  Expression,
  Literal,
  NamedExpression
}

import org.apache.spark.sql.execution.{ProjectExec, SparkPlan}
import org.apache.spark.sql.types.{DataType, Metadata}
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarBatch}

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
