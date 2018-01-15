package com.cosmos.demo.config;

import java.io.IOException;
import java.util.Properties;

public class Config {

    public static final String CONFIG_PROPERTIES = "config.properties";
    public static final Properties config = new Properties();

    static {
        try {
            config.load(Config.class.getClassLoader().getResourceAsStream(CONFIG_PROPERTIES));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private Config() {
    }

    public static Config getInstance() {
        return new Config();
    }

    public String getString(String key) {
        return config.getProperty(key);
    }

    public Integer getInteger(String key) {
        return Integer.valueOf(config.getProperty(key));
    }

}
