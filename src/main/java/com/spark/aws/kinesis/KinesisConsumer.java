package com.spark.aws.kinesis;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import org.apache.log4j.Logger;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.support.PropertiesLoaderUtils;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.KinesisClientLibDependencyException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.model.Record;

public class KinesisConsumer extends Thread implements IRecordProcessorFactory {

	final static Logger LOGGER = Logger.getLogger(KinesisConsumer.class);
	private Worker.Builder builder;

	static class CredProvider implements AWSCredentialsProvider {
		AWSCredentials _creds;

		public CredProvider(AWSCredentials creds) {
			_creds = creds;
		}

		public AWSCredentials getCredentials() {
			return _creds;
		}

		public void refresh() {
			// NOOP
		}
	}

	public KinesisConsumer(String regionName, String streamName, String appName) {

		try {
			AWSCredentialsProvider credentialsProvider = new DefaultAWSCredentialsProviderChain();

			KinesisClientLibConfiguration clientConfig = new KinesisClientLibConfiguration(appName, streamName,
					credentialsProvider, appName).withRegionName(regionName)
							.withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON)
							.withCallProcessRecordsEvenForEmptyRecordList(true);
			this.builder = new Worker.Builder().recordProcessorFactory(this).config(clientConfig);
		} catch (Exception e) {
			LOGGER.error("Exception caughtr" + e.getMessage());
			e.printStackTrace();
		}
	}

	public void run() {
		System.out.print("KinesisConsumer Start");
		Worker worker = this.builder.build();
		worker.run();

	}

	public IRecordProcessor createProcessor() {
		return new IRecordProcessor() {

			public void initialize(String shardId) {
			}

			public void processRecords(List<Record> records, IRecordProcessorCheckpointer checkpointer) {
				if (records.size() == 0)
					LOGGER.info("No data received");
				for (Record record : records) {
					byte[] bytes = new byte[record.getData().remaining()];
					record.getData().get(bytes);
					LOGGER.info("Record: " + new String(bytes));
					try {
						checkpointer.checkpoint();
					} catch (KinesisClientLibDependencyException e) {
						e.printStackTrace();
					} catch (InvalidStateException e) {
						e.printStackTrace();
					} catch (ThrottlingException e) {
						e.printStackTrace();
					} catch (ShutdownException e) {
						e.printStackTrace();
					}
				}
			}

			public void shutdown(IRecordProcessorCheckpointer checkpointer, ShutdownReason reason) {
				LOGGER.info("Shutting down the consumer");
			}
		};
	}

	public static void main(String[] args) throws IOException {
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
		KinesisConsumer consumer = new KinesisConsumer(regionName, streamName, applicationName);
		consumer.start();
	}
}