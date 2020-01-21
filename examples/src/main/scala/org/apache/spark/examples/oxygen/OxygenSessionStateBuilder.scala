// dog testing
package org.apache.spark.examples.oxygen

import org.apache.spark.annotation.{Experimental, Unstable}
import org.apache.spark.sql.{SparkSession, internal}
import org.apache.spark.sql.internal.{BaseSessionStateBuilder, SessionState, SessionStateBuilder}
import org.apache.spark.sql.execution.SparkOptimizer
import org.apache.spark.sql.hive.HiveSessionStateBuilder

//@Experimental
//@Unstable
//class OxygenSessionStateBuilder(session: SparkSession, parentState: Option[SessionState] = None)
//    extends SessionStateBuilder(session, parentState) {
//
//  override lazy val optimizer: SparkOptimizer =
//    new SparkOptimizer(catalogManager, catalog, experimentalMethods) {
//      override def postHocOptimizationBatches: Seq[Batch] = Nil
//    }
//
//  override def newBuilder: NewBuilder = new OxygenSessionStateBuilder(_, _)
//}
