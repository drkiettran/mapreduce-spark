package com.drkiettran.mapreducespark;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import scala.Tuple2;

@Service
public class FlightsByCarriersService {
	private static final Logger logger = LoggerFactory.getLogger(FlightsByCarriersService.class);

	public void getListFlightsByCarriers(JavaSparkContext sc, String host, String input, String output)
			throws IllegalArgumentException, IOException {
		logger.info(">>> starting ...");
		SparkSession ss = SparkSession.builder().appName("Java Spark Flights By Carriers").getOrCreate();
		JavaRDD<String> allRows = sc.textFile(input);
		List<String> headers = Arrays.asList(allRows.take(1).get(0).split(","));

		String field = "UniqueCarrier";

		JavaRDD<String> dataWithoutHeaders = allRows.filter(x -> !(x.split(",")[headers.indexOf(field)]).equals(field));

		JavaRDD<String> carriers = dataWithoutHeaders.map(x -> String.valueOf(x.split(",")[headers.indexOf(field)]));

		JavaPairRDD<String, Integer> flightsByCarriers = carriers.mapToPair(word -> new Tuple2<>(word, 1))
				.reduceByKey((a, b) -> a + b).sortByKey();

		for (Tuple2<String, Integer> pair : flightsByCarriers.collect()) {
			System.out.println(pair);
		}

		MRSparkUtil.deleteHdfsFile(sc, host, new Path(output));
		flightsByCarriers.saveAsTextFile(output);
		logger.info(">>> success ...");
	}
}
