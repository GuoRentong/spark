// dog testing
package org.apache.spark.examples.oxygen

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, PredicateHelper}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project, Subquery}
import org.apache.spark.sql.catalyst.rules.Rule

import scala.collection.mutable

object ExtractOxygenUDFs extends Rule[LogicalPlan] with PredicateHelper {
  def apply(plan: LogicalPlan): LogicalPlan = plan match {
    case s: Subquery if s.correlated => plan
    case _ =>
      plan transformUp {
        case plan: LogicalPlan => extract(plan)
      }
  }

  def extract(plan: LogicalPlan): LogicalPlan = {
    val udfs = collectEvaluableOxygenUDFsFromExpressions(plan.expressions)
      .filter(udf => udf.references.subsetOf(plan.inputSet))

    if (udfs.isEmpty) {
      plan
    } else {
      val attributeMap = mutable.HashMap[OxygenUDF, Expression]()
      val newChildren = plan.children.map { child =>
        val validUdfs = udfs.filter(_.references.subsetOf(child.outputSet))
        if (validUdfs.isEmpty) {
          child
        } else {
          require(
            validUdfs.forall(OxygenUDF.isScalarOxygenUDF),
            "can only extract scalar vectorized udf")
          val resultAttrs = validUdfs.zipWithIndex.map {
            case (udf, index) => AttributeReference(s"OxygenUDF$index", udf.dataType)()
          }
          val evaluation = ArrowEvalOxygen(validUdfs, resultAttrs, child)
          attributeMap ++= validUdfs.zip(resultAttrs)
          evaluation
        }
      }

      // no support for ambiguous or join in this pass
      udfs.filterNot(attributeMap.contains).foreach(udf => sys.error(s"Invalid OxygenUDF $udf"))

      val rewritten = plan.withNewChildren(newChildren).transformExpressions {
        case p: OxygenUDF if attributeMap.contains(p) => attributeMap(p)
      }

      val newPlan = extract(rewritten)

      // trim away
      if (newPlan.output != plan.output) {
        Project(plan.output, newPlan)
      } else {
        newPlan
      }
    }
  }

  private def collectEvaluableOxygenUDFsFromExpressions(
      expressions: Seq[Expression]): Seq[OxygenUDF] = {
    def collectEvaluableUDFs(expr: Expression): Seq[OxygenUDF] = expr match {
      case e: OxygenUDF => Seq(e)
      case e => e.children.flatMap(collectEvaluableUDFs)
    }
    expressions.flatMap(collectEvaluableUDFs)
  }

  private def hasScalarOxygenUDF(e: Expression): Boolean = {
    e.find(OxygenUDF.isScalarOxygenUDF).isDefined
  }
}
