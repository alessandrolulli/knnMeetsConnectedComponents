package info.debatty.java.graphs;

import java.io.Serializable;
import java.util.Comparator;

public class NeighborListComparatorASC<T> implements Comparator<Neighbor<T>>, Serializable
{
	private static final long serialVersionUID = 4245870594502451564L;

	@Override
	public int compare(Neighbor<T> a_, Neighbor<T> b_) 
	{
		return b_.compareTo(a_);
	}

}
