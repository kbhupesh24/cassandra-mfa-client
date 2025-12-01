package com.att.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;

import java.sql.Connection;
import java.sql.DriverManager;

public class JdbcMfaTest {

    public static void main(String[] args) throws Exception {

        // Ensure driver class is loaded
        Class.forName("com.att.cassandra.client.jdbc.CassandraMfaDriver");

        String url = "jdbc:cassandra-mfa://localhost:9042/DC1"
                + "?tenantId=YOUR_TENANT_ID"
                + "&clientId=YOUR_CLIENT_ID"
                + "&clientSecret=YOUR_CLIENT_SECRET"
                + "&scope=https://cassandra.azure.com/.default"
                + "&truststore=/Users/you/certs/cassandra-truststore.jks"
                + "&truststorePassword=changeit";

        try (Connection conn = DriverManager.getConnection(url)) {
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