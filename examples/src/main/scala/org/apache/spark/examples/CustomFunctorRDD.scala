
package org.apache.spark.examples

import org.apache.derby.impl.sql.compile.BooleanConstantNode
import org.apache.spark.rdd._

import org.apache.spark._

import scala.reflect.ClassTag


/**
 * An RDD that applies the provided function to every partition of the parent RDD.
 *
 * @param prev the parent RDD.
 * .@param f The function used to map a tuple of (TaskContext, partition index, input iterator) to
 *          an output iterator.
 * @param preservesPartitioning Whether the input function preserves the partitioner, which should
 *                              be `false` unless `prev` is a pair RDD and the input function
 *                              doesn't modify the keys.
 * @param isFromBarrier Indicates whether this RDD is transformed from an RDDBarrier, a stage
 *                      containing at least one RDDBarrier shall be turned into a barrier stage.
 * @param isOrderSensitive whether or not the function is order-sensitive. If it's order
 *                         sensitive, it may return totally different result when the input order
 f: (TaskContext, Int, Iterator[T]) => Iterator[U],  // (TaskContext, partition index, iterator)
 *                         is changed. Mostly stateful functions are order-sensitive.
 */
class CustomFunctorRDD[T: ClassTag](
    var prev: RDD[T],
    preservesPartitioning: Boolean = false,
    isFromBarrier: Boolean = false,
    isOrderSensitive: Boolean = false)
  extends RDD[T](prev) {
  override val partitioner = if (preservesPartitioning) firstParent[T].partitioner else None
  override def getPartitions: Array[Partition] = firstParent[T].partitions
  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    val input = firstParent[T].iterator(split, context)
    val seq = input.toSeq
    seq.foreach(x => (x.asInstanceOf[Long] + 1).asInstanceOf[T])
    seq.toIterator
  }

  override def clearDependencies(): Unit = {
    super.clearDependencies()
    prev = null
  }

  @transient protected lazy override val isBarrier_ : Boolean =
    isFromBarrier || dependencies.exists(_.rdd.isBarrier())

  override protected def getOutputDeterministicLevel = {
    if (isOrderSensitive && prev.outputDeterministicLevel == DeterministicLevel.UNORDERED) {
      DeterministicLevel.INDETERMINATE
    } else {
      super.getOutputDeterministicLevel
    }
  }
}
