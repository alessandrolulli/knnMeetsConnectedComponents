package util

@serializable
class CCPropertiesImmutable(algorithmNameFromConfig : String, 
							val datasetKNN : String, 
							val datasetCC : String,
							val outputFile : String,
							val jarPath : String, 
							val sparkMaster : String,
							val sparkPartition : Int,
							val sparkExecutorMemory : String, 
							val sparkBlockManagerSlaveTimeoutMs : String,
							val sparkCoresMax : Int,
							val sparkShuffleManager : String,
							val sparkCompressionCodec : String,
							val sparkShuffleConsolidateFiles : String,
							val sparkAkkaFrameSize : String,
							val sparkDriverMaxResultSize : String,
							val sparkExecutorInstances : Int,
							val separator : String,
							val printMessageStat : Boolean,
							val printLargestCC : Boolean,
							val printCC : Boolean,
							val printCCDistribution : Boolean,
							val printAll : Boolean,
							val customColumnValue : String,
							val switchLocal : Int,
							val switchLocalActive : Boolean,
							val vertexIdMultiplier : Int,
							val vertexNumber : Int,
							val loadBalancing : Boolean,
							val selfStar : Boolean,
							val transmitPreviousNeighbours : Boolean,
							val edgeThreshold : Double) extends Serializable
{
    val algorithmName = if(loadBalancing) algorithmNameFromConfig+"_LOAD" else algorithmNameFromConfig
	val appName = algorithmName+"_"+datasetKNN+"_"+datasetCC
	val allStat = printMessageStat && appName.contains("CRA")
	val filenameLargestCC = datasetCC+"_largestCC"
}