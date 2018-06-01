package utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertiesUtils {
    public static Properties properties;
    static{
        InputStream ios=ClassLoader.getSystemResourceAsStream("kafka.properties");
        properties=new Properties();
        try {
            properties.load(ios);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
