package util

import java.io.FileWriter
import java.text.DecimalFormat

import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import com.google.common.base.Joiner

class CCUtilIO(property : CCPropertiesImmutable) extends Serializable
{
	val fileStatDescription = "algorithmName,dataset,partition,step,timeAll,timeLoadingAndComputation,timeComputation,reduceInputMessageNumber,reduceInputSize,ccNumber,ccNumberNoIsolatedVertices,ccMaxSize,customColumn,cores,switchLocal,shuffleManager,compressionCodec,bitmaskCustom,sparkShuffleConsolidateFiles,edgeThreshold"
	val fileSimplificationDescritpion = "dataset,step,activeVertices,activeVerticesNormalized,algorithmName,activeEdges,degreeAvg,degreeMax"
	val fileTimeStep = "dataset,algorithmName,step,time,cores,switchLocal,shuffleManager,compressionCodec,bitmaskCustom,sparkShuffleConsolidateFiles"
		
	val fileStatDescriptionDiameter = "algorithmName,dataset,partition,step,timeAll,timeLoadingAndComputation,timeComputation,reduceInputMessageNumber,reduceInputSize,diameter,customColumn,cores,switchLocal,shuffleManager,compressionCodec,sparkShuffleConsolidateFiles,selfFunction,candidateFunction,loadBalancing,selfStar,transmitPreviousNeighbours,stepAll,diameterPlus"
	val fileSimplificationDescritpionDiameter = "dataset,step,activeVertices,activeVerticesNormalized,algorithmName,activeEdges,degreeAvg,degreeMax,selfFunction,candidateFunction,loadBalancing,selfStar,transmitPreviousNeighbours"
	val fileTimeStepDiameter = "dataset,algorithmName,step,time,cores,switchLocal,shuffleManager,compressionCodec,bitmaskCustom,sparkShuffleConsolidateFiles,selfFunction,candidateFunction,loadBalancing,selfStar,transmitPreviousNeighbours"
		
    def printStat( data : Long, description : String ) : Int =
    {
		printStat(data.toString, description)
    }
	
	def printStat( data : Double, description : String ) : Int =
    {
		printStat(data.toString, description)
    }
	
	def printStat( data : String, description : String ) : Int =
    {
        val printFile = new FileWriter( "time.txt", true )
        printFile.write( description + ": " + data + "\n" )
        printFile.close
        
        0
    }
	
	def printSimplification( step : Int, activeVertices : Long, initialVertices : Long , activeEdges : Double, degreeMax : Int) =
    {
		val joiner = Joiner.on(",")
		
        val printFile = new FileWriter( "simplification.txt", true )
        
		val token : Array[Object] = Array(	property.datasetCC, 
											step.toString, 
											activeVertices.toString, 
											((((activeVertices.toDouble * 100) / initialVertices)*100).round.toDouble / 100).toString, 
											property.algorithmName,
											activeEdges.toString,
											(activeEdges / activeVertices).toString,
											degreeMax.toString)
		printFile.write(joiner.join(token)+ "\n" )
		
        printFile.close
    }
	
	def printTimeStep( step : Int, time : Long) =
    {
		val joiner = Joiner.on(",")
		
        val printFile = new FileWriter( "timeStep.txt", true )
        
		// dataset, algorithmName, step, time
		val token : Array[Object] = Array(	property.datasetCC, 
											property.algorithmName, 
											step.toString, 
											time.toString,
											property.sparkCoresMax.toString,
											property.switchLocal.toString,
											property.sparkShuffleManager,
											property.sparkCompressionCodec)
		printFile.write(joiner.join(token)+ "\n" )
		
        printFile.close
    }
	
	def printMessageStep( step : Int, messageNumber : Long, messageSize : Long, bitmaskCustom : String = "000") =
    {
		val joiner = Joiner.on(",")
		
        val printFile = new FileWriter( "messageStep.txt", true )
        
		val token : Array[Object] = Array(property.datasetCC, property.algorithmName, step.toString, messageNumber.toString, messageSize.toString, bitmaskCustom, property.sparkShuffleConsolidateFiles)
		printFile.write(joiner.join(token)+ "\n" )
		
        printFile.close
    }
	
	def printStatSimple(
						value : String) =
	{
		val printFile = new FileWriter( "stats.txt", true )
		val joiner = Joiner.on(",")
		
		val token : Array[Object] = Array(	property.algorithmName, 
											property.datasetKNN, 
											value 
											)
		
		printFile.write(joiner.join(token)+ "\n" )
        printFile.close
	}
	
	def printCommonStat(
						step : Int,
						timaAll : Long,
						timeLoadingAndComputation : Long,
						timeComputation : Long,
						reduceInputMessageNumber : Long,
						reduceInputSize : Long,
						iteration : Int) =
	{
		val printFile = new FileWriter( "stats.txt", true )
		val joiner = Joiner.on(",")
		
		val desc = "algorithmName,dataset,partition,step,timeAll,timeGraph,timeComputation,messageNumber,messageSize,customColumn,cores,shuffleManager,compression,consolidateFiles,iteration"
		
		val token : Array[Object] = Array(	property.algorithmName, 
											property.datasetKNN, 
											property.sparkPartition.toString, 
											step.toString, 
											timaAll.toString, 
											timeLoadingAndComputation.toString, 
											timeComputation.toString, 
											reduceInputMessageNumber.toString, 
											reduceInputSize.toString, 
											property.customColumnValue,
											property.sparkCoresMax.toString,
											property.sparkShuffleManager,
											property.sparkCompressionCodec,
											property.sparkShuffleConsolidateFiles,
											iteration.toString)
		
		printFile.write(joiner.join(token)+ "\n" )
        printFile.close
	}
	
	def printAllStat(	algorithmName : String, 
						dataset : String,
						partition : Int, 
						step : Int,
						timaAll : Long,
						timeLoadingAndComputation : Long,
						timeComputation : Long,
						reduceInputMessageNumber : Long,
						reduceInputSize : Long,
						ccNumber : Long,
						ccNumberNoIsolatedVertices : Long,
						ccMaxSize : Int,
						customColumnValue : String,
						bitmaskCustom : String = "000") =
	{
		val printFile = new FileWriter( "stats.txt", true )
		val joiner = Joiner.on(",")
		val token : Array[Object] = Array(	algorithmName, 
											dataset, 
											partition.toString, 
											step.toString, 
											timaAll.toString, 
											timeLoadingAndComputation.toString, 
											timeComputation.toString, 
											reduceInputMessageNumber.toString, 
											reduceInputSize.toString, 
											ccNumber.toString, 
											ccNumberNoIsolatedVertices.toString, 
											ccMaxSize.toString, 
											customColumnValue,
											property.sparkCoresMax.toString,
											property.switchLocal.toString,
											property.sparkShuffleManager,
											property.sparkCompressionCodec,
											bitmaskCustom,
											property.sparkShuffleConsolidateFiles,
											property.edgeThreshold.toString)
		
		printFile.write(joiner.join(token)+ "\n" )
        printFile.close
	}
	
	def printCCDistribution(rdd : RDD[(Long, Int)]) =
	{
		val printFile = new FileWriter( "distribution.txt", true )
		val joiner = Joiner.on(",")
		
		val ccDistribution = rdd.map(t=>(t._2,1)).reduceByKey{case(a,b)=>a+b}.map(t=>t._1+","+t._2+"\n").reduce{case(a,b)=>a+b}
		
//		val token : Array[Object] = Array(algorithmName, dataset, partition.toString, hybridMessageSizeBound.toString, step.toString, timaAll.toString, timeLoadingAndComputation.toString, timeComputation.toString, reduceInputMessageNumber.toString, reduceInputSize.toString, ccNumber.toString, ccMaxSize.toString)
//		
//		printFile.write(joiner.join(token)+ "\n" )
		printFile.write(ccDistribution+ "\n" )
		
        printFile.close
	}
	
	def printCCDistributionString(rdd : RDD[(String, Int)]) =
	{
		val printFile = new FileWriter( "distribution.txt", true )
		val joiner = Joiner.on(",")
		
		val ccDistribution = rdd.map(t=>(t._2,1)).reduceByKey{case(a,b)=>a+b}.map(t=>property.datasetCC+","+t._1+","+t._2+","+property.edgeThreshold.toString+"\n").reduce{case(a,b)=>a+b}
		
//		val token : Array[Object] = Array(algorithmName, dataset, partition.toString, hybridMessageSizeBound.toString, step.toString, timaAll.toString, timeLoadingAndComputation.toString, timeComputation.toString, reduceInputMessageNumber.toString, reduceInputSize.toString, ccNumber.toString, ccMaxSize.toString)
//		
//		printFile.write(joiner.join(token)+ "\n" )
		printFile.write(ccDistribution+ "\n" )
		
        printFile.close
	}
	
	def printCC(rdd : RDD[(Long, Int)]) =
	{
		val printFile = new FileWriter( "cc.txt", true )
		val joiner = Joiner.on(",")
		
		val ccDistribution = rdd.map(t=>t._1+","+t._2+"\n").reduce{case(a,b)=>a+b}
		
//		val token : Array[Object] = Array(algorithmName, dataset, partition.toString, hybridMessageSizeBound.toString, step.toString, timaAll.toString, timeLoadingAndComputation.toString, timeComputation.toString, reduceInputMessageNumber.toString, reduceInputSize.toString, ccNumber.toString, ccMaxSize.toString)
//		
//		printFile.write(joiner.join(token)+ "\n" )
		printFile.write(ccDistribution+ "\n" )
		
        printFile.close
	}
    
    def printEdgelist( data : RDD[(Long,Long)] ) =
    {
        val collected = data.collect.iterator
        val printFile = new FileWriter( "edgelist.txt", true )
        while(collected.hasNext)
        {
            val next = collected.next
            printFile.write( next._1+" "+next._2 + "\n" )
        }
        printFile.close
    }
    
    def printFileStart(description : String) =
        {
           val printFile = new FileWriter( "time.txt", true )
            printFile.write("\n"+ description+": START\n" )
            printFile.close
        }
    
    def printFileEnd(description : String) =
        {
           val printFile = new FileWriter( "time.txt", true )
            printFile.write( description+": END\n" )
            printFile.close
        }
    
     def printTime( start : Long, end : Long, description : String ) =
        {
            val printFile = new FileWriter( "time.txt", true )
            printFile.write( description + ": " + ( end - start ) + "\n" )
            printFile.close
        }
    
    def printStep( step : Int ) =
        {
            val printFile = new FileWriter( "time.txt", true )
            printFile.write( "step: "+ step + "\n" )
            printFile.close
        }
    
    def printTimeStep( start : Long, red : Long, end : Long ) =
        {
            val printFile = new FileWriter( "time.txt", true )
            printFile.write( "blue: " + ( red - start ) + " red: " + ( end - red ) + " all: " + ( end - start ) + "\n" )
            printFile.close
        }
    
    def printToFile( file : String, data : String ) =
        {
            val printFile = new FileWriter( file, true )
            printFile.write( data )
            printFile.close
        }

}