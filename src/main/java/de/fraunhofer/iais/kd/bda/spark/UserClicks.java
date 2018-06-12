package de.fraunhofer.iais.kd.bda.spark;


import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class UserClicks {
	//main function
	public static void main(String[] args) {
		//hadoop property configuration setup
		System.setProperty("hadoop.home.dir", "E:/Project/hadoop-common-2.2.0-bin-master/");	
		String inputFile= "resources/last-fm-sample1000000.tsv";
		String appName = "WordCount";
		//Spark configuration
		SparkConf conf  = new SparkConf().setAppName(appName)
										 .setMaster("local[*]");
		//Setting spark context
		JavaSparkContext context = new JavaSparkContext(conf);
		//Read file as RDD
		JavaRDD<String> input = context.textFile(inputFile);
		//Split lines into words - tab separation and storing the Artistname
		JavaRDD<String> words = input.flatMap(line->
		{String[] parts = line.split("\t");return Arrays.asList(parts[3]).iterator();});
		//Turn the words into (word, 1) pairs
		JavaPairRDD<String, Integer> wordOne = words.mapToPair(word -> 
	{return new Tuple2<String,Integer>(word,new Integer(1));});
		//System.out.println("---------Hemanth"+wordOne);
		//Counting the number of occurances of the artist
		JavaPairRDD<String, Integer> articCount = wordOne.reduceByKey((a,b) ->  a + b);
		
		//System.out.println("---------Hemanth"+articCount.keys().toString());
		//Saving the output in file - Note:- Delete the tmp folder whenever running the code
		
		articCount.saveAsTextFile("/tmp/wordcount.txt");
		//listening events the artist “Mark Knopfler” has
		System.out.println("No. of listening events for Mark Knopfler are --> "+articCount.lookup("Mark Knopfler"));
		//Closing the spark context
		context.close();
	}
}
