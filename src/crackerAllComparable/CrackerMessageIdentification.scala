package crackerAllComparable

@serializable
class CrackerTreeMessageIdentification[T <: Comparable[T]] (val min: T, val neigh: Set[T]) extends CrackerMessageSize
{
	def voteToHalt = neigh.isEmpty
	
	def getMessageSize = neigh.size + 1
	
	def merge(other : Option[CrackerTreeMessageIdentification[T]]) : Option[CrackerTreeMessageIdentification[T]] =
	{
		if(other.isDefined)
		{
		    var minValue = min
		    if(other.get.min.compareTo(minValue) < 0) minValue = other.get.min
			Option.apply(new CrackerTreeMessageIdentification(minValue, neigh ++ other.get.neigh))
		} else
		{
			Option.apply(CrackerTreeMessageIdentification.this)
		}
	}
	
	override def toString = neigh.toString
}

object CrackerTreeMessageIdentification
{
//	def empty[T <: Comparable[T]]() = new CrackerTreeMessageIdentification[T](Option[T].empty, Set())
}