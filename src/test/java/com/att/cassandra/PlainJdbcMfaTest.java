package com.att.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

public class PlainJdbcMfaTest {

    /**
     * Run with: -Dspring.profiles.active=local
     */
    public static void main(String[] args) throws Exception {

        // Determine which properties file to load based on profile
        String profile = System.getProperty("spring.profiles.active", "");
        String propsFile = profile.isEmpty() ? "application.properties" : "application-" + profile + ".properties";

        Properties props = new Properties();
        try (InputStream is = PlainJdbcMfaTest.class.getClassLoader().getResourceAsStream(propsFile)) {
            if (is == null) {
                throw new RuntimeException(propsFile + " not found in classpath");
            }
            props.load(is);
            System.out.println("Loaded configuration from: " + propsFile);
        }

        String host = props.getProperty("cassandra.host", "localhost");
        String port = props.getProperty("cassandra.port", "9042");
        String dc = props.getProperty("cassandra.dc", "DC1");
        String tenantId = props.getProperty("azure.tenant-id");
        String clientId = props.getProperty("azure.client-id");
        String clientSecret = props.getProperty("azure.client-secret");
        String scope = props.getProperty("azure.scope");
        String sslEnabled = props.getProperty("cassandra.ssl-enabled", "false");
        String truststore = props.getProperty("cassandra.truststore");
        String truststorePassword = props.getProperty("cassandra.truststore-password");

        // Build JDBC URL from properties
        StringBuilder url = new StringBuilder();
        url.append("jdbc:cassandra-mfa://").append(host).append(":").append(port).append("/").append(dc)
           .append("?tenantId=").append(tenantId)
           .append("&clientId=").append(clientId)
           .append("&clientSecret=").append(clientSecret)
           .append("&scope=").append(scope)
           .append("&sslEnabled=").append(sslEnabled);

        if ("true".equalsIgnoreCase(sslEnabled)) {
            url.append("&truststore=").append(truststore)
               .append("&truststorePassword=").append(truststorePassword);
        }

        // Ensure driver class is loaded
        Class.forName("com.att.cassandra.client.jdbc.CassandraMfaDriver");

        try (Connection conn = DriverManager.getConnection(url.toString())) {
            System.out.println("JDBC Cassandra MFA connection established!");

            // IMPORTANT: our CassandraMfaConnection is a lightweight wrapper,
            // so for now, unwrap the underlying CqlSession:
            CqlSession session = conn.unwrap(CqlSession.class);

            session.execute("SELECT release_version FROM system.local")
                    .forEach(row ->
                            System.out.println("Cassandra version = " + row.getString("release_version")));
        }
    }
}