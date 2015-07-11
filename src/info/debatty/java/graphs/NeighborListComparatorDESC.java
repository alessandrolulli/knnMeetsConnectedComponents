package info.debatty.java.graphs;

import java.io.Serializable;
import java.util.Comparator;

public class NeighborListComparatorDESC<T> implements Comparator<Neighbor<T>>, Serializable
{
	private static final long serialVersionUID = -8502127152592156959L;

	@Override
	public int compare(Neighbor<T> a_, Neighbor<T> b_) 
	{
		return a_.compareTo(b_);
	}

}
