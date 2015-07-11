package info.debatty.java.graphs;

import info.debatty.java.util.BoundedPriorityQueue;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

/**
 *
 * @author tibo
 */
public class NeighborList<T> extends BoundedPriorityQueue<Neighbor<T>> implements Serializable {
    
	private static final long serialVersionUID = 1969183297395875761L;
	
//	public static ArrayList<Edge> Convert2Edges(HashMap<Node, NeighborList> graph) {
//        ArrayList<Edge> edges = new ArrayList<Edge>();
//        
//        for (Map.Entry<Node, NeighborList> pair : graph.entrySet()) {
//            for (Neighbor neighbor : pair.getValue()) {
//                edges.add(new Edge(pair.getKey(), neighbor.node, neighbor.similarity));
//                
//            }
//        }
//        
//        return edges;
//    }

	public NeighborList(List<Neighbor<T>> list_) 
    {
        super(list_.size() > 0 ? list_.size() : 1, new NeighborListComparatorDESC<T>());
        
        for(Neighbor<T> n : list_)
        {
        	add(n);
        }
    }
	
    public NeighborList(int size) 
    {
        super(size, new NeighborListComparatorDESC<T>());
    }
    
    public NeighborList(int size, boolean inverted_) 
    {
        super(size, inverted_ ? new NeighborListComparatorASC<T>() : new NeighborListComparatorDESC<T>());
    }
    
    public NeighborList(int size, Comparator<Neighbor<T>> comparator_) 
    {
        super(size, comparator_);
    }
    
    public NeighborList<T> convertWithSize(int size_)
    {
    	NeighborList<T> toReturn = new NeighborList<T>(size_);
    	
    	toReturn.addAll(this);
    	
    	return toReturn;
    }
    
    public List<Neighbor<T>> convertToList()
    {
    	List<Neighbor<T>> list = new ArrayList<Neighbor<T>>();
    	Iterator<Neighbor<T>> it = iterator();
    	while(it.hasNext())
    	{
    		Neighbor next = it.next();
    		list.add(next);
    	}
    	
    	return list;
    }
    
    public Neighbor getMinSimilarity()
    {
    	Neighbor toReturn = null;
    	for(Neighbor n : convertToList())
    	{
    		if(toReturn == null || n.similarity < toReturn.similarity)
    		{
    			toReturn = n;
    		}
    	}
    	return toReturn;
    }
    
    public double getAvgSimilarity()
    {
    	double sum = 0;
    	for(Neighbor n : convertToList())
    	{
    		sum += n.similarity;
    	}
    	return sum / size();
    }
    
    public Neighbor getMaxSimilarity()
    {
    	Neighbor toReturn = null;
    	for(Neighbor n : convertToList())
    	{
    		if(toReturn == null || n.similarity > toReturn.similarity)
    		{
    			toReturn = n;
    		}
    	}
    	return toReturn;
    }
    
    public String getNeighbourId()
    {
    	List<Neighbor<T>> list = convertToList();
    	StringBuilder toReturn = new StringBuilder();
    	for(Neighbor<T> n : list)
    	{
    		toReturn.append(n.node.id);
    		toReturn.append(" ");
    		toReturn.append(n.similarity);
    		toReturn.append(" ");
    	}
    	
    	return toReturn.toString();
    }
    
    /**
     * Count common values between this NeighborList and the other.
     * Both neighborlists are not modified.
     * 
     * @param other_nl
     * @return 
     */
    public int CountCommonValues(NeighborList<T> other_nl) {
        //NeighborList copy = (NeighborList) other.clone();
        ArrayList other_values = new ArrayList();
        for (Neighbor n : other_nl) {
            other_values.add(n.node.value);
        }
        
        int count = 0;
        for (Neighbor n : this) {
            Object this_value = ((Neighbor) n).node.value;
            
            for (Object other_value : other_values) {
                if ( other_value.equals(this_value)) {
                    count++;
                    other_values.remove(other_value);
                    break;
                }
            }
        }
        
        return count;
    }  
}
