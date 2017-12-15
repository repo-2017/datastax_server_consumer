package task.stefanov.datastax;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Paths;
import java.util.Properties;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class PropsUtils {

	private static final Logger logger = LogManager.getLogger(PropsUtils.class);

	public static Properties loadPropsFile(final String configFilename, final String defaultConfig)
			throws FileNotFoundException, IOException {
		Properties props = new Properties();

		InputStream stream = null;
		try {
			if (Files.exists(Paths.get(configFilename), new LinkOption[] {})) {
				stream = new FileInputStream(configFilename);
				logger.info("Found configuration file.");
			} else {
				stream = PropsUtils.class.getClassLoader().getResourceAsStream(defaultConfig);
				logger.warn("Configuration file not found - using default configuration file.");
			}
			props.load(stream);
		} finally {
			if (stream != null)
				stream.close();
		}

		return props;
	}

}
