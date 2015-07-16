package crackerAllComparable

@serializable
class CrackerTreeMessagePropagation [T <: Comparable[T]](val min : Option[T], val child : Set[T]) extends CrackerMessageSize
{
	def getMessageSize = child.size + 1
}