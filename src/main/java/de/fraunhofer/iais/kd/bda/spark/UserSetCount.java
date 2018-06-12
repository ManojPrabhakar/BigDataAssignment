package de.fraunhofer.iais.kd.bda.spark;

import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;


public class UserSetCount {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.setProperty("hadoop.home.dir", "E:/Project/hadoop-common-2.2.0-bin-master/");	
		String inputFile= "resources/last-fm-sample1000000.tsv";
		String appName = "UserCount";
	
		SparkConf conf  = new SparkConf().setAppName(appName)
										 .setMaster("local[*]");
		
		JavaSparkContext context = new JavaSparkContext(conf);
		
		//Read file
		JavaRDD<String> input = context.textFile(inputFile);
		
		
		JavaPairRDD<String, String> userOne = input.mapToPair(s -> 
	{String[] parts = s.split("\t"); return new Tuple2<String, String>(parts[3], parts[0]);});

	  JavaPairRDD<String, UserSet> userCount = userOne.aggregateByKey(new UserSet(), (a, b) -> {a.add(b); return a;}, (a, b)-> a.addUserSet(b));
	  userCount.foreach(f->{
	  System.out.print(f._1 + ":[");
	 Iterator<String> set = f._2.get().iterator();
	 
	 if (set.hasNext())
		 System.out.print(set.next());
	 while (set.hasNext()) {
	     System.out.print(", "+ set.next());
	 }
	   System.out.println("]");
	  });
	  userCount.saveAsTextFile("F:/tmp1/usercountnew.txt");
	//Gets the userSet of Mark Knopfler
	  System.out.println("userSet of Mark Knopfler is : ->"+userCount.lookup("Mark Knopfler"));
	  context.close();

	}
}

