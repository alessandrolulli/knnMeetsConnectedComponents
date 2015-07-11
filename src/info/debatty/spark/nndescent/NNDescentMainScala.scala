package info.debatty.spark.nndescent

import util.CCPropertiesImmutable
import org.apache.spark.api.java.JavaSparkContext
import util.CCUtil
import org.apache.spark.rdd.RDD
import util.CCProperties
import info.debatty.java.graphs.Node
import info.debatty.java.stringsimilarity.JaroWinkler
import org.apache.spark.api.java.JavaPairRDD
import info.debatty.java.graphs.NeighborListFactory
import info.debatty.java.graphs.NeighborList
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters._

object NNDescentMainScala 
{
	def main( args_ : Array[String] ) : Unit = 
    {
	    val timeBegin = System.currentTimeMillis()
	    
	    val propertyLoad = new CCProperties("NNDESCENT", args_(0)).load();
		val property = propertyLoad.getImmutable;
		val nnDescentK = propertyLoad.getInt("nnDescentK", 10);
		val nnDescentMaxIteration = propertyLoad.getInt("nnDescentMaxIteration", 20);
		val nnDescentMinIteration = propertyLoad.getInt("nnDescentMinIteration", 5);
		val nnDescentOneFile = propertyLoad.getBoolean("nnDescentOneFile", false);
		val nnDescentMaxDistance = propertyLoad.getBoolean("nnDescentMaxDistance", false);
		
		val util = new CCUtil(property);
		val sc = util.getJavaSparkContext();
		
		val file = sc.textFile(property.dataset, property.sparkPartition)
		val vertexRDD = util.loadVertexMail(file).map(t => new Node[String](t._1, t._2))
		
        val jaroWinkler = new JaroWinkler
        
        var iteration = nnDescentMinIteration
        var nndes = new NNDescent[String]
        nndes = nndes.setK(nnDescentK)
        nndes = nndes.setMaxIterations(nnDescentMinIteration)
        nndes = nndes.setSimilarity(jaroWinkler);
        
        var graph = nndes.initializeAndComputeGraph(vertexRDD.toJavaRDD, new NeighborListFactory, property.sparkPartition);
        
        val graphSize = graph.cache().count;
        val timeEnd = System.currentTimeMillis()
        
        util.io.printCommonStat(0, 
                				timeEnd-timeBegin, 
                				timeEnd-timeBegin, 
                				timeEnd-timeBegin, 
                				0, 
                				0,
                				iteration)
        
        val graphScala = JavaPairRDD.toRDD(graph)

        val toPrint2 = graphScala.map(t => t._1.id+"\t"+t._2.getNeighbourId())
        
        if(nnDescentOneFile)
        	toPrint2.coalesce(1, true).saveAsTextFile(property.outputFile+"_ITER_"+iteration)
        else
            toPrint2.saveAsTextFile(property.outputFile+"_ITER_"+iteration)
            
        nndes = nndes.setMaxIterations(1)
        var timeTotal = timeEnd-timeBegin
        while(nnDescentMaxIteration > iteration)
        {
            iteration = iteration + 1
            
            val timeStartIteration = System.currentTimeMillis()
            graph = nndes.computeGraph(graph, new NeighborListFactory);
        
	        val graphSize = graph.cache().count;
	        val timeEndIteration = System.currentTimeMillis()
	        
	        timeTotal = timeTotal + (timeEndIteration - timeStartIteration)
	        
	        util.io.printCommonStat(0, 
	                				timeTotal, 
	                				timeTotal, 
	                				timeTotal, 
	                				0, 
	                				0,
	                				iteration)
	        
	        val graphScala = JavaPairRDD.toRDD(graph)
	
	        val toPrint2 = graphScala.map(t => t._1.id+"\t"+t._2.getNeighbourId())
	        
	        if(property.printAll || iteration % 5 == 0)
	        {
		        if(nnDescentOneFile)
		        	toPrint2.coalesce(1, true).saveAsTextFile(property.outputFile+"_ITER_"+iteration)
		        else
		            toPrint2.saveAsTextFile(property.outputFile+"_ITER_"+iteration)
	        }
        }
    }
}