package com.att.cassandra.jdbc;

import com.att.cassandra.client.AzureAdAuthProvider;
import com.att.cassandra.client.AzureAdTokenProvider;
import com.att.cassandra.client.SslUtil;
import com.att.cassandra.client.jdbc.CassandraMfaConnection;
import com.att.cassandra.client.jdbc.CassandraUrl;
import com.datastax.oss.driver.api.core.CqlSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.sql.*;
import java.util.Properties;

public class CassandraMfaDriver implements Driver {

    private static final Logger log = LoggerFactory.getLogger(CassandraMfaDriver.class);

    static {
        try {
            log.info("Registering CassandraMfaDriver with DriverManager");
            DriverManager.registerDriver(new CassandraMfaDriver());
            log.info("CassandraMfaDriver registered successfully");
        } catch (SQLException e) {
            log.error("Failed to register CassandraMfaDriver", e);
            throw new RuntimeException("Failed to register CassandraMfaDriver", e);
        }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            log.trace("URL not accepted by CassandraMfaDriver: {}", url);
            return null;
        }

        log.info("Connecting to Cassandra via JDBC URL: {}", sanitizeUrl(url));
        log.debug("Parsing JDBC URL");
        CassandraUrl parsed = CassandraUrl.parse(url, info);

        log.debug("Creating AzureAdTokenProvider for tenant: {}", parsed.tenantId);
        AzureAdTokenProvider tokenProvider = new AzureAdTokenProvider(
                parsed.tenantId,
                parsed.clientId,
                parsed.clientSecret,
                parsed.scope
        );

        try {
            log.info("Building CqlSession to {}:{} in datacenter '{}', ssl: {}", parsed.host, parsed.port, parsed.localDc, parsed.sslEnabled);
            var builder = CqlSession.builder()
                    .addContactPoint(new InetSocketAddress(parsed.host, parsed.port))
                    .withLocalDatacenter(parsed.localDc)
                    .withAuthProvider(new AzureAdAuthProvider(tokenProvider));

            if (parsed.sslEnabled) {
                log.debug("SSL enabled, truststore: {}", parsed.truststore);
                builder.withSslContext(SslUtil.createSslContext(parsed.truststore, parsed.truststorePassword));
            }

            CqlSession session = builder.build();
            log.info("JDBC connection established successfully");
            return new CassandraMfaConnection(session);
        } catch (Exception e) {
            log.error("Failed to create Cassandra MFA connection to {}:{}", parsed.host, parsed.port, e);
            throw new SQLException("Unable to create Cassandra MFA connection", e);
        }
    }

    @Override
    public boolean acceptsURL(String url) {
        boolean accepts = url != null && url.startsWith("jdbc:cassandra-mfa://");
        log.trace("acceptsURL({}) = {}", url, accepts);
        return accepts;
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
        log.trace("getPropertyInfo called for URL: {}", url);
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        return 1;
    }

    @Override
    public int getMinorVersion() {
        return 0;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public java.util.logging.Logger getParentLogger() {
        return java.util.logging.Logger.getLogger("CassandraMfaDriver");
    }

    private String sanitizeUrl(String url) {
        // Remove sensitive parameters from URL for logging
        if (url == null) return null;
        return url.replaceAll("clientSecret=[^&]*", "clientSecret=***")
                  .replaceAll("truststorePassword=[^&]*", "truststorePassword=***");
    }
}