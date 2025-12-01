package com.att.cassandra.client;

import java.io.InputStream;
import java.util.Properties;

public class Config {

    private static final Properties props = new Properties();

    static {
        try (InputStream in = Config.class.getClassLoader().getResourceAsStream("application.properties")) {
            props.load(in);
        } catch (Exception e) {
            throw new RuntimeException("Unable to load application.properties", e);
        }
    }

    public static String get(String key) {
        return props.getProperty(key);
    }
}