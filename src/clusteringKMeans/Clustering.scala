package clusteringKMeans

import java.io.FileNotFoundException
import scala.collection.JavaConverters.seqAsJavaListConverter
import scala.collection.mutable.MultiMap
import scala.util.Random
import org.apache.spark.SparkContext.doubleRDDToDoubleRDDFunctions
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.rdd.RDD
import info.debatty.java.graphs.Neighbor
import info.debatty.java.graphs.NeighborList
import info.debatty.java.graphs.NeighborListFactory
import info.debatty.java.graphs.Node
import info.debatty.java.stringsimilarity.JaroWinkler
import info.debatty.spark.nndescent.NNDescent
import util.CCProperties
import util.CCPropertiesImmutable
import util.CCUtil
import scala.collection.JavaConversions._
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.feature.Word2VecModel
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.feature.Word2Vec
import org.apache.spark.mllib.clustering.KMeans

object Clustering
{
    def loadDataset(data : RDD[String], property : CCPropertiesImmutable) : RDD[(String, String)] = // return (id, subject) OR (id, seed)
	{
		val toReturn = data.map(line =>
			{
				val splitted = line.split(property.separator)
				if (splitted.size > 1) 
				{
					(splitted(0), splitted(1))
				} else 
				{
					throw new FileNotFoundException("impossible to parse: "+line)
				}
			})
			
		toReturn
	}
    
    def sumArray( m : Array[Double], n : Array[Double] ) : Array[Double] = {
        for ( i <- 0 until m.length ) { m( i ) += n( i ) }
        return m
    }

    def divArray( m : Array[Double], divisor : Double ) : Array[Double] = {
        for ( i <- 0 until m.length ) { m( i ) /= divisor }
        return m
    }

    def wordToVector( w : String, m : Word2VecModel ) : Vector = {
        try {
            return m.transform( w )
        } catch {
            case e : Exception => return Vectors.zeros( 100 )
        }
    }
   
	def main( args_ : Array[String] ) : Unit = 
    {
    	// code from : http://bigdatasciencebootcamp.com/posts/Part_3/clustering_news.html
	    
	    val timeStart = System.currentTimeMillis()
	    val propertyLoad = (new CCProperties("CLUSTERING", args_(0))).load();
		val property = propertyLoad.getImmutable;
		val k = propertyLoad.getInt("K", 39000);
		
		val util = new CCUtil(property);
		val sc = util.getSparkContext();
		
		val data = util.loadVertexMail(sc.textFile(property.dataset, property.sparkPartition)) 
		    
		val news_titles = data.map(t => (t._1, t._2.split(" ").toSeq))
		val all_input = news_titles.map(t => t._2).flatMap(x => x).map(x => Seq(x))
		
		val word2vec = new Word2Vec()
		val model = word2vec.fit(all_input)
		
		val title_vectors = news_titles.map(x => x._2.map(m => wordToVector(m, model).toArray)).filter(t => !t.isEmpty)
		.map(t => new DenseVector(divArray(t.reduceLeft(sumArray),t.length)).asInstanceOf[Vector])
		
		val title_pairs = news_titles.map(x => (x, x._2.map(m => wordToVector(m, model).toArray))).filter(t => !t._2.isEmpty)
		.map(t => (t._1, new DenseVector(divArray(t._2.reduceLeft(sumArray),t._2.length)).asInstanceOf[Vector]))

		var numClusters = k
		val numIterations = 25
		var clusters = KMeans.train(title_vectors, numClusters, numIterations)
		
		val article_membership = title_pairs.mapValues(x => clusters.predict(x)).cache
		article_membership.first
		
		val timeEnd = System.currentTimeMillis()

		val toPrint = article_membership.map(x => x._1._1+"\t"+x._2)
		toPrint.coalesce(1, true).saveAsTextFile(property.outputFile)
		
		util.io.printStatSimple((timeEnd - timeStart).toString)
		
    }
}