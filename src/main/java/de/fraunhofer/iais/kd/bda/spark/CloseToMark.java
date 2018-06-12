package de.fraunhofer.iais.kd.bda.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class CloseToMark {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		UserSet u = new UserSet();
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
	  double dist = u.distanceTo(f._2);
		 System.out.print(dist);
	   System.out.println("]");
	  });
	  //userCount.saveAsTextFile("resources/usercountnew.txt");
	   context.close();

	}

}
