package com.att.cassandra.client.jdbc;

import com.att.cassandra.client.AzureAdAuthProvider;
import com.att.cassandra.client.AzureAdTokenProvider;
import com.att.cassandra.client.SslUtil;
import com.datastax.oss.driver.api.core.CqlSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.sql.*;
import java.util.Properties;

/**
 * JDBC Driver for Cassandra with Azure AD Multi-Factor Authentication (MFA) support.
 *
 * <p>This driver enables JDBC-based applications to connect to Cassandra clusters
 * using Azure AD JWT tokens for authentication. It wraps the DataStax Java Driver
 * and provides a standard JDBC interface.</p>
 *
 * <h2>JDBC URL Format:</h2>
 * <pre>
 * jdbc:cassandra-mfa://host:port/datacenter?tenantId=...&clientId=...&clientSecret=...&scope=...
 * </pre>
 *
 * <h2>URL Parameters:</h2>
 * <ul>
 *   <li><b>tenantId</b> - Azure AD tenant ID (required)</li>
 *   <li><b>clientId</b> - Azure AD application client ID (required)</li>
 *   <li><b>clientSecret</b> - Azure AD client secret (required)</li>
 *   <li><b>scope</b> - OAuth scope, e.g., "api://{app-id}/.default" (required)</li>
 *   <li><b>sslEnabled</b> - Enable SSL/TLS (default: false)</li>
 *   <li><b>truststore</b> - Path to JKS truststore (required if SSL enabled)</li>
 *   <li><b>truststorePassword</b> - Truststore password (required if SSL enabled)</li>
 * </ul>
 *
 * <h2>Usage Example:</h2>
 * <pre>{@code
 * // Load the driver
 * Class.forName("com.att.cassandra.client.jdbc.CassandraMfaDriver");
 *
 * // Connect using JDBC URL
 * String url = "jdbc:cassandra-mfa://localhost:9042/datacenter1" +
 *     "?tenantId=your-tenant-id" +
 *     "&clientId=your-client-id" +
 *     "&clientSecret=your-secret" +
 *     "&scope=api://your-app-id/.default";
 *
 * try (Connection conn = DriverManager.getConnection(url)) {
 *     // Use the connection
 *     CqlSession session = conn.unwrap(CqlSession.class);
 *     session.execute("SELECT * FROM system.local");
 * }
 * }</pre>
 *
 * <h2>Note:</h2>
 * <p>This driver provides limited JDBC functionality. For full CQL capabilities,
 * use {@code conn.unwrap(CqlSession.class)} to access the underlying DataStax session.</p>
 *
 * @see CassandraMfaConnection
 * @see CassandraUrl
 */
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