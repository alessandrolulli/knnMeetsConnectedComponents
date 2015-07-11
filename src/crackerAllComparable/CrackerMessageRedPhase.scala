package crackerAllComparable

@serializable
class CrackerTreeMessageRedPhase [T <: Comparable[T]](val first : Option[CrackerTreeMessageIdentification[T]], val second : Option[CrackerTreeMessageTree[T]]) extends CrackerMessageSize
{
	def getMessageSize = 0//first.getOrElse(CrackerTreeMessageIdentification.empty).getMessageSize + second.getOrElse(CrackerTreeMessageTree.empty).getMessageSize 
}

object CrackerTreeMessageRedPhase
{
	def apply[T <: Comparable[T]](first : CrackerTreeMessageIdentification[T]) = new CrackerTreeMessageRedPhase[T](Option.apply(first), Option.empty)
	def apply[T <: Comparable[T]](second : CrackerTreeMessageTree[T]) = new CrackerTreeMessageRedPhase[T](Option.empty, Option.apply(second))
}