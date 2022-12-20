package com.brekken.kafka;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.Properties;

public class App {
    public static void main(String[] args) {
        Properties properties = loadProperties("application.properties");
        new Streams(properties).start();
    }

    public static Properties loadProperties(String propertiesFileName) {
        Properties configuration = new Properties();
        InputStream inputStream = Streams.class
                .getClassLoader()
                .getResourceAsStream(propertiesFileName);
        Objects.requireNonNull(inputStream);
        try {
            configuration.load(inputStream);
        } catch (IOException ex) {
            throw new RuntimeException("Cannot load properties", ex);
        }

        return configuration;
    }
}