package util;

import info.debatty.spark.nndescent.NNDescentMainScala;
import crackerAllComparable.CrackerAllComparable;

public class KnnMeetsConnectedComponents 
{
	public static void main(String[] args_)
	{
		if(args_.length > 0)
		{
			NNDescentMainScala.main(args_);
			CrackerAllComparable.main(args_);
		} else
		{
			System.out.println("ERROR Command input must be: command algorithmName configFile");
		}
	}
}
