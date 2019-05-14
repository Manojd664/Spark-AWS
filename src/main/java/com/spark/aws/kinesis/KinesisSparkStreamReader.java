package com.spark.aws.kinesis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kinesis.KinesisUtils;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.support.PropertiesLoaderUtils;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;

public class KinesisSparkStreamReader {

	final static Logger LOGGER = Logger.getLogger(KinesisSparkStreamReader.class);

	private static void validateStream(AmazonKinesis kinesisClient, String streamName) {
		try {
			LOGGER.info("Validation of stream start");
			DescribeStreamResult result = kinesisClient.describeStream(streamName);
			if (!"ACTIVE".equals(result.getStreamDescription().getStreamStatus())) {
				LOGGER.error("Stream " + streamName + " is not active. Please wait a few moments and try again.");
				System.exit(1);
			}
		} catch (ResourceNotFoundException e) {
			LOGGER.error("Stream " + streamName + " does not exist. Please create it in the console.");
			e.printStackTrace();
			System.exit(1);
		} catch (Exception e) {
			LOGGER.error("Error found while describing the stream " + streamName);
			e.printStackTrace();
			System.exit(1);
		}
		LOGGER.info("Validation of stream end");
	}

	public static void readFromStream(String regionName, String streamName, String applicationName, String endPointUrl)
			throws InterruptedException {

		AmazonKinesisClient kinesisClient = new AmazonKinesisClient();
		kinesisClient.setEndpoint(endPointUrl);
		validateStream(kinesisClient, streamName);
		int numShards = kinesisClient.describeStream(streamName).getStreamDescription().getShards().size();

		int numStreams = numShards;
		Duration batchInterval = new Duration(10000);

		Duration kinesisCheckpointInterval = batchInterval;

		SparkConf sparkConfig = new SparkConf().setAppName(applicationName).setMaster("local[4]");

		JavaStreamingContext jssc = new JavaStreamingContext(sparkConfig, batchInterval);
		List<JavaDStream<byte[]>> streamsList = new ArrayList<>();
		for (int i = 0; i < numStreams; i++) {
			streamsList.add(KinesisUtils.createStream(jssc, applicationName, streamName, endPointUrl, regionName,
					InitialPositionInStream.TRIM_HORIZON, kinesisCheckpointInterval, StorageLevel.MEMORY_AND_DISK_2()));
		}
		JavaDStream<byte[]> kinesisStream;
		if (streamsList.size() > 1) {
			kinesisStream = jssc.union(streamsList.get(0), streamsList.subList(1, streamsList.size()));
		} else {
			kinesisStream = streamsList.get(0);
		}
		kinesisStream.foreachRDD(new VoidFunction<JavaRDD<byte[]>>() {
			@Override
			public void call(JavaRDD<byte[]> rdd) throws Exception {
				LOGGER.info("Displaying Records");
				rdd.collect().forEach(x -> System.out.println(new String(x)));
			}
		});
		jssc.start();
		jssc.awaitTermination();
	}

	public static void main(String[] args) throws InterruptedException, IOException {
		System.setProperty("hadoop.home.dir", "D:\\winutils");
		LOGGER.info("Stating kinesis consumer");
		LOGGER.info("loading properties start");
		ClassPathResource resource = new ClassPathResource("application.properties");
		Properties props = null;
		try {
			props = PropertiesLoaderUtils.loadProperties(resource);
		} catch (IOException e) {
			LOGGER.error("Unable to load application.properties");
		}
		LOGGER.info("loading properties end");
		String regionName = props.getProperty("kinesis-region-name");
		String streamName = props.getProperty("kinesis-consumer-stream-name");
		String applicationName = props.getProperty("kinesis-consumer-application-name");
		String endPointUrl = props.getProperty("kinesis-endpoint-url");

		readFromStream(regionName, streamName, applicationName, endPointUrl);
	}

}
