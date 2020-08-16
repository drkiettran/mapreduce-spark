package com.drkiettran.mapreducespark;

import static java.lang.System.exit;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.Banner;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class MapreduceSparkApplication implements CommandLineRunner {
	private static final Logger logger = LoggerFactory.getLogger(MapreduceSparkApplication.class);

	@Value("${mapreduce.host}")
	private String mapreduceHost;

	@Autowired
	private WordCountService wcs;

	@Autowired
	private FlightsByCarriersService fbc;

	private static SparkConf sparkConf;
	private static JavaSparkContext sc;

	public static void getSparkContext() {
		sparkConf = new SparkConf().setAppName("Spark RDD Example").setMaster("local[2]").set("spark.executor.memory",
				"2g");

		sc = new JavaSparkContext(sparkConf);

	}

	public static void main(String[] args) {
		// disabled banner, don't want to see the spring logo
		SpringApplication app = new SpringApplication(MapreduceSparkApplication.class);
		app.setBannerMode(Banner.Mode.OFF);
		getSparkContext();
		app.run(args);
	}

	@Override
	public void run(String... args) throws Exception {
		if (args.length < 3) {
			logger.error("*** Must provide cmd input output ***");
			exit(-1);
		}

		logger.info("running ... {}", args[0]);
		String inputPath = mapreduceHost + args[1];
		String outputPath = mapreduceHost + args[2];

		if ("wc".equals(args[0])) {
			wcs.countWords(sc, mapreduceHost, inputPath, outputPath);
		} else if ("fbc".equals(args[0])) {
			fbc.getListFlightsByCarriers(sc, mapreduceHost, inputPath, outputPath);
		} else {
			logger.error("*** Invalid command {} ***", args[0]);
		}

		exit(0);
	}
}
