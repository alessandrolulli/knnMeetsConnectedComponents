package crackerAllComparable

import org.apache.spark.SparkContext._
import scala.collection.mutable.ListBuffer
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import java.io.FileWriter
import util.CCPropertiesImmutable

@serializable
class CrackerAlgorithm[T <: Comparable[T]](property : CCPropertiesImmutable) {
	def mapPropagate(item : (T, CrackerTreeMessagePropagation[T])) : Iterable[(T, CrackerTreeMessagePropagation[T])] =
		{
			var outputList : ListBuffer[(T, CrackerTreeMessagePropagation[T])] = new ListBuffer
			if (item._2.min.isDefined) {
				outputList.prepend((item._1, new CrackerTreeMessagePropagation(item._2.min, Set())))
				val it = item._2.child.iterator
				while (it.hasNext) {
					val next = it.next
					outputList.prepend((next, new CrackerTreeMessagePropagation(item._2.min, Set())))
				}
			} else {
				outputList.prepend(item)
			}
			outputList
		}

	def reducePropagate(item1 : CrackerTreeMessagePropagation[T], item2 : CrackerTreeMessagePropagation[T]) : CrackerTreeMessagePropagation[T] =
		{
			var minEnd = item1.min
			if (minEnd.isEmpty) minEnd = item2.min

			new CrackerTreeMessagePropagation(minEnd, item1.child ++ item2.child)
		}

	def emitBlue(item : (T, CrackerTreeMessageIdentification[T]), forceLoadBalancing : Boolean) : Iterable[(T, CrackerTreeMessageIdentification[T])] =
		{
			var outputList : ListBuffer[(T, CrackerTreeMessageIdentification[T])] = new ListBuffer
			if (item._2.min == item._1 && (item._2.neigh.isEmpty || (item._2.neigh.size == 1 && item._2.neigh.contains(item._1)))) {
				//                outputList.prepend( ( item._1, new CrackerTreeMessage( item._2.min, Set()) ) )
			} else {

				val min = item._2.min

				if (item._2.neigh.isEmpty) {
					outputList.prepend((item._1, new CrackerTreeMessageIdentification(min, Set())))
				} else {
					outputList.prepend((item._1, new CrackerTreeMessageIdentification(min, Set(min))))
				}

				if (min.compareTo(item._1)<0 || !forceLoadBalancing) {
					val it = item._2.neigh.iterator
					while (it.hasNext) {
						val next = it.next
						outputList.prepend((next, new CrackerTreeMessageIdentification(min, Set(min))))
					}
				}
			}

			outputList.toIterable
		}
	
	def emitRed(item : (T, CrackerTreeMessageIdentification[T])) : Iterable[(T, CrackerTreeMessageRedPhase[T])] = {

		emitRed(item, false)
	}

	def emitRed(item : (T, CrackerTreeMessageIdentification[T]), forceLoadBalancing : Boolean) : Iterable[(T, CrackerTreeMessageRedPhase[T])] = {

		var outputList : ListBuffer[(T, CrackerTreeMessageRedPhase[T])] = new ListBuffer

		val minset : Set[T] = item._2.neigh
		if (minset.size > 1) {
			if(property.loadBalancing || forceLoadBalancing)
			{
				outputList.prepend((item._2.min, CrackerTreeMessageRedPhase.apply(new CrackerTreeMessageIdentification(item._2.min, Set(item._2.min)))))
			}
		    else
		    {
		        outputList.prepend((item._2.min, CrackerTreeMessageRedPhase.apply(new CrackerTreeMessageIdentification(item._2.min, minset))))
		    }	
			var it = minset.iterator
			while (it.hasNext) {
				val value : T = it.next
				if (value != item._2.min)
					outputList.prepend((value, CrackerTreeMessageRedPhase.apply(new CrackerTreeMessageIdentification(item._2.min, Set(item._2.min)))))
			} 
		} else if (minset.size == 1 && minset.contains(item._1)) {
			outputList.prepend((item._1, CrackerTreeMessageRedPhase.apply(new CrackerTreeMessageIdentification(item._1, Set()))))
		}

		if (!item._2.neigh.contains(item._1)) {
			outputList.prepend((item._2.min, CrackerTreeMessageRedPhase.apply(new CrackerTreeMessageTree(Option.empty, Set(item._1)))))
			outputList.prepend((item._1, CrackerTreeMessageRedPhase.apply(new CrackerTreeMessageTree(Option.apply(item._2.min), Set()))))
		}

		outputList.toIterable
	}

	def reduceBlue(item1 : CrackerTreeMessageIdentification[T], item2 : CrackerTreeMessageIdentification[T]) : CrackerTreeMessageIdentification[T] =
		{
			val ret = item1.neigh ++ item2.neigh
			var min = item1.min
			if(item2.min.compareTo(item1.min)<0) min = item2.min

			new CrackerTreeMessageIdentification(min, ret)
		}

	def mergeMessageIdentification(first : Option[CrackerTreeMessageIdentification[T]], second : Option[CrackerTreeMessageIdentification[T]]) : Option[CrackerTreeMessageIdentification[T]] =
		{
			if (first.isDefined) {
				first.get.merge(second)
			} else {
				second
			}
		}

	def mergeMessageTree(first : Option[CrackerTreeMessageTree[T]], second : Option[CrackerTreeMessageTree[T]]) : Option[CrackerTreeMessageTree[T]] =
		{
			if (first.isDefined) {
				first.get.merge(second)
			} else {
				second
			}
		}

	def reduceRed(item1 : CrackerTreeMessageRedPhase[T], item2 : CrackerTreeMessageRedPhase[T]) : CrackerTreeMessageRedPhase[T] =
		{
			new CrackerTreeMessageRedPhase(mergeMessageIdentification(item1.first, item2.first), mergeMessageTree(item1.second, item2.second))
		}

	def mergeTree(start : Option[RDD[(String, CrackerTreeMessageTree[String])]], add : RDD[(String, CrackerTreeMessageTree[String])], crackerUseUnionInsteadOfJoin : Boolean, crackerForceEvaluation : Boolean) : Option[RDD[(String, CrackerTreeMessageTree[String])]] =
		{
			if (start.isDefined) {
				if(crackerUseUnionInsteadOfJoin)
				{
					Option.apply(start.get.union(add))
				} else
				{
					if(crackerForceEvaluation)
					{
						val treeUpdated = start.get.leftOuterJoin(add).map(t => (t._1, t._2._1.merge(t._2._2).get))
						val forceEvaluation = treeUpdated.count
						Option.apply(treeUpdated)
					} else
					{
						Option.apply(start.get.leftOuterJoin(add).map(t => (t._1, t._2._1.merge(t._2._2).get)))
					}
				}
			} else {
				Option.apply(add)
			}
		}
	
	def mergeTree(spark : SparkContext, start : Option[RDD[(String, CrackerTreeMessageTree[String])]], add : Array[(String, CrackerTreeMessageTree[String])], crackerUseUnionInsteadOfJoin : Boolean, crackerForceEvaluation : Boolean) : Option[RDD[(String, CrackerTreeMessageTree[String])]] =
		{
			if (start.isDefined) {
				if(crackerUseUnionInsteadOfJoin)
				{
					Option.apply(start.get.union(spark.parallelize(add)))
				} else
				{
					if(crackerForceEvaluation)
					{
						val treeUpdated = start.get.leftOuterJoin(spark.parallelize(add)).map(t => (t._1, t._2._1.merge(t._2._2).get))
						val forceEvaluation = treeUpdated.count
						Option.apply(treeUpdated)
					} else
					{
						Option.apply(start.get.leftOuterJoin(spark.parallelize(add)).map(t => (t._1, t._2._1.merge(t._2._2).get)))
					}
				}
			} else {
				Option.apply(spark.parallelize(add))
			}
		}
	
	def mergeTree(start : Option[Array[(Long, CrackerTreeMessageTree[String])]], add : Array[(Long, CrackerTreeMessageTree[String])]) : Option[Array[(Long, CrackerTreeMessageTree[String])]] =
		{
			if (start.isDefined) {
				Option.apply(start.get.union(add))
			} else {
				Option.apply(add)
			}
		}

	def reducePrepareDataForPropagation(a : CrackerTreeMessageTree[T], b : CrackerTreeMessageTree[T]) : CrackerTreeMessageTree[T] =
		{
			var parent = a.parent
			if (parent.isEmpty) parent = b.parent

			new CrackerTreeMessageTree(parent, a.child ++ b.child)
		}
	
	def getMessageNumberForPropagation(step : Int, vertexNumber : Long) =
	{
		val stepPropagation = (step - 1) / 2
		
		(vertexNumber * stepPropagation) + vertexNumber
	}
	
	def getMessageSizeForPropagation(step : Int, vertexNumber : Long) =
	{
		val stepPropagation = (step - 1) / 2
		
		((vertexNumber * 2) * stepPropagation) - vertexNumber
	}
}