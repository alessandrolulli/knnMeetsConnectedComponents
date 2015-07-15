package util;

public class Main 
{
	public static void main(String[] args_)
	{
		if(args_.length > 0)
		{
			info.debatty.spark.nndescent.NNDescentMainScala.main(args_);
			crackerAllComparable.CrackerAllComparable.main(args_);
		} else
		{
			System.out.println("ERROR Command input must be: command algorithmName configFile");
		}
	}
}
