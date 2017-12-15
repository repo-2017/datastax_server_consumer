package task.stefanov.datastax;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * 
 * @author stefanov
 */
public class LogAggregationServer {

	public static final String DEFAULT_SERVER_PROPS_CONF = "default-server.props";
	public static final String SERVER_PROPS_CONFIG = "server.props";
	public static final String DEFAULT_CONSUMER_PROPS_CONF = "default-consumer.props";
	public static final String CONSUMER_PROPS_CONFIG_NAME = "consumer.props";
	public static final String OUT_DIR_PROP = "output.dir";
	public static final String TOPICS_PROP = "topics";

	private static final Logger logger = LogManager.getLogger(LogAggregationServer.class);

	private final ExecutorService consumers = Executors.newCachedThreadPool();

	private String[] topics;
	private String outputDir;
	private Properties consumerProps;

	LogAggregationServer() {
	}

	LogAggregationServer(String[] topics, String outputDir, Properties consumerProps) {
		this.topics = topics;
		this.outputDir = outputDir;
		this.consumerProps = consumerProps;
	}

	public void startListening() {
		logger.info("Subscribing for configured topics.");

		for (String topic : topics) {

			Consumer<String, String> kafkaConsumer = new KafkaConsumer<>(consumerProps);
			Thread consumer = new Thread(new TailingConsumer(topic, kafkaConsumer, outputDir));
			consumers.submit(consumer);
		}
	}

	public String[] getTopics() {
		return topics;
	}

	public void setTopics(String[] topics) {
		this.topics = topics;
	}

	public String getOutputDir() {
		return outputDir;
	}

	public void setOutputDir(String outputDir) {
		this.outputDir = outputDir;
	}

	public ExecutorService getConsumers() {
		return consumers;
	}

	public Properties getConsumerProps() {
		return consumerProps;
	}

	public void setConsumerProps(Properties consumerProps) {
		this.consumerProps = consumerProps;
	}

}
