package com.heramb.sparksort;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class SparkSort {

	public static void main(String[] args) {
		SparkConf config_spk = new SparkConf().setAppName("SparkSort").setMaster("local");
		JavaSparkContext javaSpkCont = new JavaSparkContext(config_spk);

		JavaRDD<String> Rdd = javaSpkCont.textFile(args[0]);

		long start = System.currentTimeMillis();
		JavaPairRDD<String, String> pair_java_Rdd = Rdd.mapToPair(new PairFunction<String, String, String>() {
			@Override
			public Tuple2<String, String> call(String str) throws Exception {
				String key = str.substring(0, 9);
				String value = str.substring(10);
				return new Tuple2<String, String>(key, value);
			}
		});

		JavaPairRDD<String, String> Rdd_sort = pair_java_Rdd.sortByKey();

		Rdd_sort.saveAsTextFile("/tmp/SparkSort.out");
		javaSpkCont.close();

		long end = System.currentTimeMillis();
		long TotalTime = end - start;
		System.out.println("Total time elapsed: " + TotalTime);
	}
}