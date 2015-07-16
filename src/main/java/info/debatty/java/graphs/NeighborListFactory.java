package info.debatty.java.graphs;

import java.io.Serializable;
import java.util.Comparator;

public class NeighborListFactory implements Serializable
{
	private static final long serialVersionUID = 3637625262337648884L;

	private final Comparator<Neighbor> _comparator;
	
	public NeighborListFactory()
	{
		_comparator = new NeighborListComparatorDESC();
	}
	
	public NeighborListFactory(Comparator<Neighbor> comparator_)
	{
		_comparator = comparator_;
	}
	
	public NeighborList create(int size_)
	{
		return new NeighborList(size_, _comparator);
	}

}
