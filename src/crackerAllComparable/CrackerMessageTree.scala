package crackerAllComparable

@serializable
class CrackerTreeMessageTree[T <: Comparable[T]] (val parent : Option[T], val child : Set[T]) extends CrackerMessageSize
{
	def getMessageSize = child.size + 1
	
	def merge(other : Option[CrackerTreeMessageTree[T]]) : Option[CrackerTreeMessageTree[T]] =
	{
		if(other.isDefined)
		{
			var parentNew = parent
			
			if(parentNew.isEmpty)
			{
				parentNew = other.get.parent
			}
			
			Option.apply(new CrackerTreeMessageTree(parentNew, child ++ other.get.child))
		} else
		{
			Option.apply(CrackerTreeMessageTree.this)
		}
	}
	
	def merge(other : CrackerTreeMessageTree[T]) : CrackerTreeMessageTree[T] =
	{
		var parentNew = parent
		
		if(parentNew.isEmpty)
		{
			parentNew = other.parent
		}
		
		new CrackerTreeMessageTree(parentNew, child ++ other.child)
	}
	
	def getMessagePropagation(id : T) = 
	{
		if(parent.isEmpty)
		{
			new CrackerTreeMessagePropagation[T](Option.apply(id), child)
		} else
		{
			new CrackerTreeMessagePropagation[T](Option.empty, child)
		}
	}
}

object CrackerTreeMessageTree
{
	def empty = new CrackerTreeMessageTree(Option.empty, Set())
}