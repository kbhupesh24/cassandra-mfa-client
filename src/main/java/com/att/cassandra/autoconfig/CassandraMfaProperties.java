package com.att.cassandra.autoconfig;

import org.springframework.boot.context.properties.ConfigurationProperties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties
public class CassandraMfaProperties {
    private String tenantId;
    private String clientId;
    private String clientSecret;
    private String scope;
    private String host;
    private int port = 9042;
    private String localDc;
    private boolean sslEnabled = false;
    private String truststore;
    private String truststorePassword;
}