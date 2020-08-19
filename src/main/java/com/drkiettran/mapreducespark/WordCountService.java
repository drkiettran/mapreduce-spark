package com.drkiettran.mapreducespark;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import scala.Tuple2;

@Service
public class WordCountService {
	private static final Logger logger = LoggerFactory.getLogger(WordCountService.class);

	public void countWords(JavaSparkContext sc, String host, String input, String output) throws IOException {
		System.out.println("counting ...");

		JavaRDD<String> textFile = sc.textFile(input);
		JavaPairRDD<String, Integer> counts = textFile
				.flatMap(s -> Arrays.asList(s.replaceAll("[^A-Za-z0-9 ]", " ").split(" ")).iterator())
				.mapToPair(word -> new Tuple2<>(word, 1)).reduceByKey((a, b) -> a + b).sortByKey();

		MRSparkUtil.deleteHdfsFile(sc, host, new Path(output));
		counts.saveAsTextFile(output);
		logger.info(">>> Success!");

	}
}
