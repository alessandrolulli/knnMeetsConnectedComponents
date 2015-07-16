package info.debatty.spark.nndescent;

import info.debatty.java.graphs.Neighbor;
import info.debatty.java.graphs.NeighborList;
import info.debatty.java.graphs.NeighborListFactory;
import info.debatty.java.graphs.Node;
import info.debatty.java.graphs.SimilarityInterface;

import java.io.Serializable;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Random;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * Implementation of NN-Descent k-nn graph building algorithm.
 * Based on the paper "Efficient K-Nearest Neighbor Graph Construction for Generic Similarity Measures"
 * by Dong et al.
 * http://www.cs.princeton.edu/cass/papers/www11.pdf
 * 
 * NN-Descent works by iteratively exploring the neighbors of neighbors...
 * 
 * @author Thibault Debatty
 */
public class NNDescent<T> implements Serializable {
    

	private static final long serialVersionUID = 7019440109943920667L;
	private int max_iterations = 10;
    private int k = 10;
    private SimilarityInterface<T> similarity;
    
    public NNDescent()
    {
    	
    }
    
    /**
     * Set k (the number of edges per node).
     * Default value is 10
     * @param k
     * @return this
     */
    public NNDescent<T> setK(int k) {
        if (k <= 0) {
            throw new InvalidParameterException("k must be positive!");
        }
        
        this.k = k;
        return this;
    }
    
    /**
     * Set the maximum number of iterations.
     * Default value is 10
     * @param max_iterations
     * @return 
     */
    public NNDescent<T> setMaxIterations(int max_iterations) {
        if (max_iterations <= 0) {
//            throw new InvalidParameterException("max_iterations must be positive!");
        }
        
        this.max_iterations = max_iterations;
        return this;
    }
    
    /**
     * Define how similarity will be computed between node values.
     * NNDescent can use any similarity (even non metric).
     * @param similarity
     * @return this
     */
    public NNDescent<T> setSimilarity(SimilarityInterface<T> similarity) {
        this.similarity = similarity;
        return this;
    }
    
    /**
     * Compute and return the graph.
     * @param nodes
     * @return 
     */
    public JavaPairRDD<Node<T>, NeighborList<T>> computeGraph(JavaPairRDD<Node<T>, NeighborList<T>> graph, final NeighborListFactory neighborListFactory_)
    {
    	for (int iteration = 0; iteration < max_iterations; iteration++) 
    	{
            // Reverse
            JavaPairRDD<Node<T>, Node<T>> exploded_graph = graph.flatMapToPair(
                    new PairFlatMapFunction<Tuple2<Node<T>, NeighborList<T>>, Node<T>, Node<T>>() {
                
                public Iterable<Tuple2<Node<T>, Node<T>>> call(Tuple2<Node<T>, NeighborList<T>> tuple) throws Exception {
                    
                    ArrayList<Tuple2<Node<T>, Node<T>>> r = new ArrayList<Tuple2<Node<T>, Node<T>>>();
                    
                    for (Neighbor<T> neighbor : tuple._2()) {

                        r.add(new Tuple2<Node<T>, Node<T>>(tuple._1(), neighbor.node));
                        r.add(new Tuple2<Node<T>, Node<T>>(neighbor.node, tuple._1()));
                    }
                    return r;
                    
                }
            });
            
            exploded_graph.cache().first();
            graph.unpersist();
            
            // 
            graph = exploded_graph.groupByKey().flatMapToPair(
                    new PairFlatMapFunction<Tuple2<Node<T>, Iterable<Node<T>>>, Node<T>, NeighborList<T>>() {

                public Iterable<Tuple2<Node<T>, NeighborList<T>>> call(Tuple2<Node<T>, Iterable<Node<T>>> tuple) throws Exception {
                    
                    // Fetch all nodes
                    ArrayList<Node<T>> nodes = new ArrayList<Node<T>>();
                    nodes.add(tuple._1);
                    
                    for (Node<T> n : tuple._2) {
                        nodes.add(n);
                    }
                    
                    // 
                    ArrayList<Tuple2<Node<T>, NeighborList<T>>> r = new ArrayList<Tuple2<Node<T>, NeighborList<T>>>(nodes.size());
                    
                    for (Node<T> n : nodes) {
                        NeighborList<T> nl = neighborListFactory_.create(Math.min(k/2,3));
                        
                        for (Node<T> other : nodes) {
                            if (other.equals(n)) {
                                continue;
                            }
                            
                            nl.add(new Neighbor<T>(
                                    other,
                                    similarity.similarity(n.value, other.value)));
                        }
                        
                        r.add(new Tuple2<Node<T>, NeighborList<T>>(n, nl));
                    }
                    
                    return r;
                    
                }
            });
            
            graph.cache().first();
            
            // Filter
            graph = graph.groupByKey().mapToPair(new PairFunction<Tuple2<Node<T>, Iterable<NeighborList<T>>>, Node<T>, NeighborList<T>>() {

                public Tuple2<Node<T>, NeighborList<T>> call(Tuple2<Node<T>, Iterable<NeighborList<T>>> tuple) throws Exception {
                    NeighborList nl = neighborListFactory_.create(k);
                    
                    for (NeighborList<T> other : tuple._2()) {
                    	nl.addAll(other);
                    }
                    
                    return new Tuple2<Node<T>, NeighborList<T>>(tuple._1, nl);
                }
            });
            
            graph.cache().first();
            exploded_graph.unpersist();
        }
        
        return graph;
    }
    
    public JavaPairRDD<Node<T>, NeighborList<T>> initializeAndComputeGraph(JavaRDD<Node<T>> nodes, final NeighborListFactory neighborListFactory_, final int partitionNumber) 
    {
        if (similarity == null) {
            throw new InvalidParameterException("Similarity is not defined!");
        }
        
        JavaPairRDD<Integer, Node<T>> randomized = nodes.flatMapToPair(
                new PairFlatMapFunction<Node<T>, Integer, Node<T>>() {
            
            Random rand = new Random();
            
            public Iterable<Tuple2<Integer, Node<T>>> call(Node<T> n) throws Exception {
                ArrayList<Tuple2<Integer, Node<T>>> r = new ArrayList<Tuple2<Integer, Node<T>>>();
                for (int i = 0; i < 10; i++) {
                    r.add(new Tuple2<Integer, Node<T>>(rand.nextInt(partitionNumber), n));
                }
                
                return r;
            }
        });
        
        // Inside bucket, associate
        JavaPairRDD<Node<T>, NeighborList<T>> random_nl = randomized.groupByKey().flatMapToPair(
        		new PairFlatMapFunction<Tuple2<Integer, Iterable<Node<T>>>, Node<T>, NeighborList<T>>() {
        			Random rand = new Random();
                    public Iterable<Tuple2<Node<T>, NeighborList<T>>> call(Tuple2<Integer, Iterable<Node<T>>> tuple) throws Exception {
                    
                ArrayList<Node<T>> nodes = new ArrayList<Node<T>>();
                for (Node<T> n : tuple._2) {
                    nodes.add(n);
                }

                ArrayList<Tuple2<Node<T>, NeighborList<T>>> r = new ArrayList<Tuple2<Node<T>, NeighborList<T>>>();
                for (Node<T> n : nodes) {
                    NeighborList<T> nnl = new NeighborList<T>(k);
                    for (int i = 0; i < k; i++) {
                        nnl.add(new Neighbor<T>(
                                nodes.get(rand.nextInt(nodes.size())),
                                Double.MAX_VALUE));
                    }

                    r.add(new Tuple2<Node<T>, NeighborList<T>>(n, nnl));
                }

                return r;
            }
        });
        
        // Merge
        JavaPairRDD<Node<T>, NeighborList<T>> graph = random_nl.reduceByKey(
                
                new Function2<NeighborList<T>, NeighborList<T>, NeighborList<T>>() {
            
            public NeighborList<T> call(NeighborList<T> nl1, NeighborList<T> nl2) throws Exception {
                NeighborList<T> nnl = new NeighborList<T>(k);
                nnl.addAll(nl1);
                nnl.addAll(nl2);
                return nnl;
            }
        });
        
        graph.cache().first();
        randomized.unpersist();
        random_nl.unpersist();
        
        return computeGraph(graph, neighborListFactory_);
    }
}
