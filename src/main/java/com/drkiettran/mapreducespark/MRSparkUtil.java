package com.drkiettran.mapreducespark;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaSparkContext;

public class MRSparkUtil {
	public static boolean deleteHdfsFile(JavaSparkContext sc, String host, Path path) throws IOException {
		FileSystem hdfs = FileSystem.get(URI.create(host), sc.hadoopConfiguration());
		return hdfs.delete(path, true);
	}
}
