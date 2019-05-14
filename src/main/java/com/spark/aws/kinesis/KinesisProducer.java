package com.spark.aws.kinesis;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.log4j.Logger;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.support.PropertiesLoaderUtils;
import com.amazonaws.AmazonClientException;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;

public class KinesisProducer {

	final static Logger LOGGER = Logger.getLogger(KinesisProducer.class);

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

	private static void sendSingleRecordToStream(String event, AmazonKinesis kinesisClient, String streamName) {
		byte[] bytes = event.getBytes();
		if (bytes == null) {
			LOGGER.warn("Could not get JSON bytes for event");
			return;
		}
		PutRecordRequest putRecord = new PutRecordRequest();
		putRecord.setStreamName(streamName);
		putRecord.setPartitionKey(RandomStringUtils.randomAlphanumeric(30));
		putRecord.setData(ByteBuffer.wrap(bytes));

		try {
			PutRecordResult putResult = kinesisClient.putRecord(putRecord);
			LOGGER.info("Stream: " + streamName + ", " + putResult);
		} catch (AmazonClientException ex) {
			LOGGER.warn("Error sending record to Amazon Kinesis.", ex);
		}
	}

	private static void sendMultipleRecordsToStream(List<String> events, AmazonKinesis kinesisClient,
			String streamName) {
		PutRecordsRequest putRecordsRequest = new PutRecordsRequest();
		putRecordsRequest.setStreamName(streamName);
		List<PutRecordsRequestEntry> putRecordsRequestEntryList = new ArrayList<>();
		for (int i = 0; i < events.size(); i++) {
			PutRecordsRequestEntry putRecordsRequestEntry = new PutRecordsRequestEntry();
			putRecordsRequestEntry.setData(ByteBuffer.wrap(events.get(i).getBytes()));
			putRecordsRequestEntry.setPartitionKey(RandomStringUtils.randomAlphanumeric(30));
			putRecordsRequestEntryList.add(putRecordsRequestEntry);
		}
		putRecordsRequest.setRecords(putRecordsRequestEntryList);
		PutRecordsResult putRecordsResult = kinesisClient.putRecords(putRecordsRequest);
		LOGGER.info("Stream "+streamName+", Put Result" + putRecordsResult);
	}

	public static void main(String[] args) {
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

		Region region = RegionUtils.getRegion(regionName);
		if (region == null) {
			System.err.println(regionName + " is not a valid AWS region.");
			System.exit(1);
		}
		AmazonKinesisClient kinesisClient = new AmazonKinesisClient();

		kinesisClient.setRegion(region);
		validateStream(kinesisClient, streamName);
		
		LOGGER.info("Pushing single record to the stream");
		sendSingleRecordToStream("Message1:Hello,How are you?", kinesisClient, streamName);
		
		LOGGER.info("Pushing multiple record to the stream");
		List<String> records=Arrays.asList("Record1: Hello, How are you?","Record2:Hello, I am good. How about you?");
		sendMultipleRecordsToStream(records, kinesisClient, streamName);

	}
}
