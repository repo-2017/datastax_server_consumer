package task.stefanov.datastax;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collections;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * 
 * @author stefanov
 *
 */
public class TailingConsumer implements Runnable {

	private static final Logger logger = LogManager.getLogger(TailingConsumer.class);

	final private String topic;
	final private Consumer<String, String> consumer;
	final private String outputDir;

	public TailingConsumer(String topic, Consumer<String, String> consumer, String outputDir) {
		this.topic = topic;
		this.consumer = consumer;
		this.outputDir = outputDir;
	}

	@Override
	public void run() {
		logger.info("Subscribing for topic: " + topic);

		PrintWriter writer = null;
		try {

			consumer.subscribe(Collections.singletonList(topic));
			File copiedLog = new File(getOutputDir(), getTopic());
			logger.info("Creating copied log to write messages : " + copiedLog.getAbsolutePath());

			writer = new PrintWriter(new File(getOutputDir(), getTopic()));
			while (true) {

				if (Thread.interrupted())
					throw new InterruptedException();

				final ConsumerRecords<String, String> consumerRecords = consumer.poll(1000);
				for (ConsumerRecord<String, String> record : consumerRecords) {
					if (logger.isDebugEnabled())
						logger.debug("Message received : " + record.toString());
					writer.println(record.value());
				}
				writer.flush();
				consumer.commitAsync();

				try {
					Thread.sleep(10);
				} catch (InterruptedException e) {
				}
			}

		} catch (IOException ex) {

			logger.error("Listening failed on topic : " + topic, ex);
		} catch (InterruptedException e1) {

			if (writer != null && consumer != null) {
				writer.flush();
				consumer.commitAsync();
			}
		} finally {

			if (consumer != null)
				consumer.close();
			if (writer != null)
				writer.close();
		}
	}

	public String getTopic() {
		return topic;
	}

	public String getOutputDir() {
		return outputDir;
	}
	
	public Consumer<String, String> getConsumer() {
		return consumer;
	}

}
