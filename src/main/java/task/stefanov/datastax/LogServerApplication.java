package task.stefanov.datastax;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * 
 * @author stefanov
 *
 */
public class LogServerApplication {

	private static final Logger logger = LogManager.getLogger(LogServerApplication.class);

	public static void main(String[] args) {
		logger.info("Starting Aggregation server application ...");

		try {
			logger.info("Reading server application configuration properties ...");
			Properties serverProps = PropsUtils.loadPropsFile(LogAggregationServer.SERVER_PROPS_CONFIG,
					LogAggregationServer.DEFAULT_SERVER_PROPS_CONF);
			String topicsProp = (String) serverProps.get(LogAggregationServer.TOPICS_PROP);
			String[] topics = topicsProp.split(";");
			String outputDir = (String) serverProps.get(LogAggregationServer.OUT_DIR_PROP);

			logger.info("Reading Kafka consumer configuration properties ...");
			Properties consumerProps = PropsUtils.loadPropsFile(LogAggregationServer.CONSUMER_PROPS_CONFIG_NAME,
					LogAggregationServer.DEFAULT_CONSUMER_PROPS_CONF);

			// create, configure and start application
			final LogAggregationServer logServer = new LogAggregationServer(topics, outputDir, consumerProps);

			Runtime.getRuntime().addShutdownHook(new Thread() {
				@Override
				public void run() {
					super.run();
					logServer.getConsumers().shutdown();
					try {
						logServer.getConsumers().awaitTermination(3, TimeUnit.SECONDS);
					} catch (InterruptedException e) {
					} finally {
					}

				}
			});

			logServer.startListening();

		} catch (IOException ex) {
			logger.fatal("Failed starting log aggregation application", ex);
		}
	}

}
