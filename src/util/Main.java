package util;

public class Main 
{
	public static void main(String[] args_)
	{
		if(args_.length > 1)
		{
			String algorithmName = args_[0];
			String[] argsParsed = new String[args_.length - 1];
					
			System.arraycopy( args_, 1, argsParsed, 0, args_.length - 1 );
			
			switch(algorithmName)
			{
			case "CRACKER" : 
			{
				crackerAllComparable.CrackerAllComparable.main(argsParsed);
				break;
			}
			case "KNN" : 
			{
				info.debatty.spark.nndescent.NNDescentMainScala.main(argsParsed);
				break;
			}
			default : 
			{
				System.out.println("ERROR: Algorithm name not recognized");
				break;
			}
			}
			
		} else
		{
			System.out.println("ERROR Command input must be: command algorithmName configFile");
		}
	}
}
