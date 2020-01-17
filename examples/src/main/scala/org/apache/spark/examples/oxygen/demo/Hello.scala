// fucker
//package org.apache.spark.examples.oxygen
//
//class Hello {}
//
///*
// * Licensed to the Apache Software Foundation (ASF) under one or more
// * contributor license agreements.  See the NOTICE file distributed with
// * this work for additional information regarding copyright ownership.
// * The ASF licenses this file to You under the Apache License, Version 2.0
// * (the "License"); you may not use this file except in compliance with
// * the License.  You may obtain a copy of the License at
// *
// *    http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//import org.apache.spark.TaskContext
//import org.apache.spark.internal.Logging
//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.catalyst.expressions.{
//  Add,
//  Alias,
//  AttributeReference,
//  BindReferences,
//  Expression,
//  Literal,
//  NamedExpression
//}
//import org.apache.spark.sql.catalyst.rules.Rule
//import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
//import org.apache.spark.sql.execution.{ColumnarRule, ProjectExec, SparkPlan}
//import org.apache.spark.sql.types.LongType
//import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarBatch}
//import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
//
///**
// * An Iterator that insures that the batches [[ColumnarBatch]]s it iterates over are all closed
// * properly.
// */
//class CloseableColumnBatchIterator(
//    itr: Iterator[ColumnarBatch],
//    f: ColumnarBatch => ColumnarBatch)
//    extends Iterator[ColumnarBatch] {
//  var cb: ColumnarBatch = null
//
//  override def hasNext: Boolean = {
//    closeCurrentBatch()
//    itr.hasNext
//  }
//
//  TaskContext
//    .get()
//    .addTaskCompletionListener[Unit]((tc: TaskContext) => {
//      closeCurrentBatch()
//    })
//
//  private def closeCurrentBatch(): Unit = {
//    if (cb != null) {
//      cb.close
//      cb = null
//    }
//  }
//
//  override def next(): ColumnarBatch = {
//    closeCurrentBatch()
//    cb = f(itr.next())
//    cb
//  }
//}
//
///**
// * A version of ProjectExec that adds in columnar support.
// */
//class ColumnarProjectExec(projectList: Seq[NamedExpression], child: SparkPlan)
//    extends ProjectExec(projectList, child) {
//
//  override def supportsColumnar: Boolean = true
//
//  // Disable code generation
//  override def supportCodegen: Boolean = false
//
//  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
//    val boundProjectList = BindReferences.bindReferences(projectList, child.output)
//    val rdd = child.executeColumnar()
//    rdd.mapPartitions(
//      (itr) =>
//        new CloseableColumnBatchIterator(
//          itr,
//          (cb) => {
//            val newColumns = boundProjectList
//              .map(expr =>
//                expr.asInstanceOf[ColumnarExpression].columnarEval(cb).asInstanceOf[ColumnVector])
//              .toArray
//            new ColumnarBatch(newColumns, cb.numRows())
//          }))
//  }
//
//  override def hashCode(): Int = super.hashCode() + "ColumnarProject".hashCode
//  // We have to override equals because subclassing a case class like ProjectExec is not that clean
//  // One of the issues is that the generated equals will see ColumnarProjectExec and ProjectExec
//  // as being equal and this can result in the withNewChildren method not actually replacing
//  // anything
//  override def equals(other: Any): Boolean = {
//    if (!super.equals(other)) return false
//    other.isInstanceOf[ColumnarProjectExec]
//  }
//}
//
///**
// * A version of add that supports columnar processing for longs.
// */
//class ColumnarAdd(left: Expression, right: Expression) extends Add(left, right) with Logging {
//  override def supportsColumnar(): Boolean = left.supportsColumnar && right.supportsColumnar
//
//  override def columnarEval(batch: ColumnarBatch): Any = {
//    var lhs = null
//    var rhs = null
//    var ret = null
//    try {
//      lhs = left.columnarEval(batch)
//      rhs = right.columnarEval(batch)
//
//      if (lhs == null || rhs == null) ret = null
//      else if (lhs.isInstanceOf[ColumnVector] && rhs.isInstanceOf[ColumnVector]) {
//        val l = lhs.asInstanceOf[ColumnVector]
//        val r = rhs.asInstanceOf[ColumnVector]
//        val result = new OnHeapColumnVector(batch.numRows(), dataType)
//        ret = result
//
//        for (i <- 0 until batch.numRows()) {
//          result.appendLong(l.getLong(i) + r.getLong(i))
//        }
//      } else if (rhs.isInstanceOf[ColumnVector]) {
//        val l = lhs.asInstanceOf[Long]
//        val r = rhs.asInstanceOf[ColumnVector]
//        val result = new OnHeapColumnVector(batch.numRows(), dataType)
//        ret = result
//
//        for (i <- 0 until batch.numRows()) {
//          result.appendLong(l + r.getLong(i))
//        }
//      } else if (lhs.isInstanceOf[ColumnVector]) {
//        val l = lhs.asInstanceOf[ColumnVector]
//        val r = rhs.asInstanceOf[Long]
//        val result = new OnHeapColumnVector(batch.numRows(), dataType)
//        ret = result
//
//        for (i <- 0 until batch.numRows()) {
//          result.appendLong(l.getLong(i) + r)
//        }
//      } else ret = nullSafeEval(lhs, rhs)
//    } finally {
//      if (lhs != null && lhs.isInstanceOf[ColumnVector]) lhs.asInstanceOf[ColumnVector].close()
//      if (rhs != null && rhs.isInstanceOf[ColumnVector]) rhs.asInstanceOf[ColumnVector].close()
//    }
//    ret
//  }
//
//  // Again we need to override equals because we are subclassing a case class
//  override def equals(other: Any): Boolean = {
//    if (!super.equals(other)) return false
//    other.isInstanceOf[ColumnarAdd]
//  }
//
//  override def hashCode(): Int = super.hashCode() + 123
//}
//
//case class ColumnarOverrides() extends Rule[SparkPlan] {
//  def replaceWithColumnarExpression(exp: Expression): Expression = exp match {
//    case a: Alias =>
//      Alias(replaceWithColumnarExpression(a.child), a.name)(
//        a.exprId,
//        a.qualifier,
//        a.explicitMetadata)
//    case att: AttributeReference =>
//      att // No sub expressions and already supports columnar so just return it
//    case lit: Literal =>
//      lit // No sub expressions and already supports columnar so just return it
//    case add: Add
//        if (add.dataType == LongType) &&
//          (add.left.dataType == LongType) &&
//          (add.right.dataType == LongType) =>
//      // Add only supports Longs for now.
//      new ColumnarAdd(
//        replaceWithColumnarExpression(add.left),
//        replaceWithColumnarExpression(add.right))
//    case exp =>
//      logWarning(
//        s"Columnar Processing for expression ${exp.getClass} ${exp} is not currently supported.")
//      exp
//  }
//
//  def replaceWithColumnarPlan(plan: SparkPlan): SparkPlan = plan match {
//    case plan: ProjectExec =>
//      new ColumnarProjectExec(
//        plan.projectList.map((exp) =>
//          replaceWithColumnarExpression(exp).asInstanceOf[NamedExpression]),
//        replaceWithColumnarPlan(plan.child))
//    case p =>
//      logWarning(s"Columnar Processing for ${p.getClass} is not currently supported.")
//      p.withNewChildren(p.children.map(replaceWithColumnarPlan))
//  }
//
//  def apply(plan: SparkPlan): SparkPlan = replaceWithColumnarPlan(plan)
//}
//
//case class ColumnarOverrideRules(session: SparkSession) extends ColumnarRule with Logging {
//  val overrides = ColumnarOverrides()
//
//  override def pre: Rule[SparkPlan] = plan => {
//    if (columnarEnabled) overrides(plan) else plan
//  }
//
//  def columnarEnabled =
//    session.sqlContext.getConf("org.apache.spark.example.columnar.enabled", "true").trim.toBoolean
//}
//
///**
// * Extension point to enable columnar processing.
// *
// * To run with columnar set spark.sql.extensions to org.apache.spark.example.Plugin
// */
//class Plugin extends Function1[SparkSessionExtensions, Unit] with Logging {
//  override def apply(extensions: SparkSessionExtensions): Unit = {
//    logWarning(
//      "Installing extensions to enable columnar CPU support." +
//        " To disable this set `org.apache.spark.example.columnar.enabled` to false")
//    extensions.injectColumnar((session) => ColumnarOverrideRules(session))
//  }
//}
