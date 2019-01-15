package utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ResourceUtils {
	private static final String LOCAL_PROPERTIES_FILE_PATH = "/profile/local/system.properties";
	private static final String ONLINE_PROPERTIES_FILE_PATH = "/profile/online/system.properties";
	private static final Properties PROPERTIES = new Properties();

	static {
		init();
	}

	private static void init() {
		InputStream onlineStream = ResourceUtils.class.getResourceAsStream(ONLINE_PROPERTIES_FILE_PATH);
		InputStream localStream = ResourceUtils.class.getResourceAsStream(LOCAL_PROPERTIES_FILE_PATH);
		try {
			if (localStream != null) {
				PROPERTIES.load(localStream);
			}
			else if (onlineStream != null) {
				PROPERTIES.load(onlineStream);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static String getProperty(String key) {

        return PROPERTIES.getProperty(key);
	}

	public static void main(String[] args) {
		String property = ResourceUtils.getProperty("zookeeper.connect");
		System.out.println(property);
	}
}
