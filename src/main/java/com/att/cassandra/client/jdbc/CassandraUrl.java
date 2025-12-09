package com.att.cassandra.client.jdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Parses JDBC URLs for the Cassandra MFA driver.
 *
 * <p>This class extracts connection parameters from JDBC URLs in the format:</p>
 * <pre>
 * jdbc:cassandra-mfa://host:port/datacenter?param1=value1&param2=value2
 * </pre>
 *
 * <h2>URL Components:</h2>
 * <ul>
 *   <li><b>host</b> - Cassandra node hostname or IP address</li>
 *   <li><b>port</b> - CQL native transport port (default: 9042)</li>
 *   <li><b>datacenter</b> - Local datacenter name for the driver</li>
 * </ul>
 *
 * <h2>Query Parameters:</h2>
 * <table border="1">
 *   <tr><th>Parameter</th><th>Required</th><th>Description</th></tr>
 *   <tr><td>tenantId</td><td>Yes</td><td>Azure AD tenant ID</td></tr>
 *   <tr><td>clientId</td><td>Yes</td><td>Azure AD application client ID</td></tr>
 *   <tr><td>clientSecret</td><td>Yes</td><td>Azure AD client secret</td></tr>
 *   <tr><td>scope</td><td>Yes</td><td>OAuth scope for token request</td></tr>
 *   <tr><td>sslEnabled</td><td>No</td><td>Enable SSL/TLS (default: false)</td></tr>
 *   <tr><td>truststore</td><td>If SSL</td><td>Path to JKS truststore file</td></tr>
 *   <tr><td>truststorePassword</td><td>If SSL</td><td>Truststore password</td></tr>
 * </table>
 *
 * <h2>Example URLs:</h2>
 * <pre>
 * // Without SSL
 * jdbc:cassandra-mfa://localhost:9042/datacenter1?tenantId=...&clientId=...&clientSecret=...&scope=...
 *
 * // With SSL
 * jdbc:cassandra-mfa://cassandra.example.com:9042/dc1?tenantId=...&clientId=...&clientSecret=...&scope=...&sslEnabled=true&truststore=/path/to/truststore.jks&truststorePassword=changeit
 * </pre>
 *
 * @see CassandraMfaDriver
 */
public final class CassandraUrl {

    private static final Logger log = LoggerFactory.getLogger(CassandraUrl.class);

    public final String host;
    public final int port;
    public final String localDc;

    public final String tenantId;
    public final String clientId;
    public final String clientSecret;
    public final String scope;

    public final boolean sslEnabled;
    public final String truststore;
    public final String truststorePassword;

    private CassandraUrl(String host,
                         int port,
                         String localDc,
                         String tenantId,
                         String clientId,
                         String clientSecret,
                         String scope,
                         boolean sslEnabled,
                         String truststore,
                         String truststorePassword) {

        this.host = host;
        this.port = port;
        this.localDc = localDc;
        this.tenantId = tenantId;
        this.clientId = clientId;
        this.clientSecret = clientSecret;
        this.scope = scope;
        this.sslEnabled = sslEnabled;
        this.truststore = truststore;
        this.truststorePassword = truststorePassword;

        log.debug("CassandraUrl created - host: {}, port: {}, dc: {}, tenant: {}, ssl: {}", host, port, localDc, tenantId, sslEnabled);
    }

    /**
     * Example URL:
     *
     * jdbc:cassandra-mfa://localhost:9042/DC1
     *   ?tenantId=...&clientId=...&clientSecret=...&scope=...&
     *    truststore=/path/to/truststore.jks&truststorePassword=changeit
     */
    public static CassandraUrl parse(String url, Properties info) throws SQLException {
        log.debug("Parsing JDBC URL");

        if (url == null || !url.startsWith("jdbc:cassandra-mfa://")) {
            log.error("Invalid URL format: {}", url);
            throw new SQLException("Invalid URL for CassandraMfaDriver: " + url);
        }

        String rest = url.substring("jdbc:cassandra-mfa://".length()); // host:port/DC1?...
        String hostPortPart;
        String dcPart = null;
        String queryPart = null;

        int slashIdx = rest.indexOf('/');
        if (slashIdx >= 0) {
            hostPortPart = rest.substring(0, slashIdx);  // host:port
            int qIdx = rest.indexOf('?', slashIdx);
            if (qIdx >= 0) {
                dcPart = rest.substring(slashIdx + 1, qIdx);
                queryPart = rest.substring(qIdx + 1);
            } else {
                dcPart = rest.substring(slashIdx + 1);
            }
        } else {
            hostPortPart = rest;
        }

        String host;
        int port = 9042;
        int colonIdx = hostPortPart.indexOf(':');
        if (colonIdx >= 0) {
            host = hostPortPart.substring(0, colonIdx);
            String portStr = hostPortPart.substring(colonIdx + 1);
            try {
                port = Integer.parseInt(portStr);
            } catch (NumberFormatException e) {
                log.error("Invalid port number: {}", portStr);
                throw new SQLException("Invalid port in Cassandra URL: " + portStr, e);
            }
        } else {
            host = hostPortPart;
        }

        log.trace("Parsed host: {}, port: {}", host, port);

        Map<String, String> params = parseQuery(queryPart);
        // Fallbacks to Properties if not present in URL
        String tenantId = get(params, info, "tenantId");
        String clientId = get(params, info, "clientId");
        String clientSecret = get(params, info, "clientSecret");
        String scope = get(params, info, "scope");

        String sslEnabledStr = get(params, info, "sslEnabled");
        boolean sslEnabled = "true".equalsIgnoreCase(sslEnabledStr);
        String truststore = get(params, info, "truststore");
        String truststorePassword = get(params, info, "truststorePassword");

        if (tenantId == null || clientId == null || clientSecret == null || scope == null) {
            log.error("Missing required Azure AD parameters - tenantId: {}, clientId: {}, scope: {}",
                    tenantId != null, clientId != null, scope != null);
            throw new SQLException("Missing required Azure AD parameters in URL or properties (tenantId, clientId, clientSecret, scope)");
        }

        String localDc = (dcPart != null && !dcPart.isEmpty()) ? dcPart : get(params, info, "localDc");
        if (localDc == null) {
            log.error("Missing localDc parameter");
            throw new SQLException("Missing localDc in Cassandra URL or properties");
        }

        if (sslEnabled && (truststore == null || truststorePassword == null)) {
            log.error("SSL enabled but missing SSL parameters - truststore: {}, truststorePassword: {}",
                    truststore != null, truststorePassword != null);
            throw new SQLException("SSL enabled but missing truststore or truststorePassword in URL or properties");
        }

        log.info("JDBC URL parsed successfully - connecting to {}:{} in datacenter '{}', ssl: {}", host, port, localDc, sslEnabled);

        return new CassandraUrl(
                host,
                port,
                localDc,
                tenantId,
                clientId,
                clientSecret,
                scope,
                sslEnabled,
                truststore,
                truststorePassword
        );
    }

    private static Map<String, String> parseQuery(String query) {
        Map<String, String> map = new HashMap<>();
        if (query == null || query.isEmpty()) {
            return map;
        }
        String[] pairs = query.split("&");
        for (String pair : pairs) {
            int idx = pair.indexOf('=');
            if (idx > 0 && idx < pair.length() - 1) {
                String key = pair.substring(0, idx);
                String value = pair.substring(idx + 1);
                map.put(key, value);
            }
        }
        return map;
    }

    private static String get(Map<String, String> params, Properties info, String key) {
        String fromParams = params.get(key);
        if (fromParams != null) return fromParams;
        if (info != null) {
            return info.getProperty(key);
        }
        return null;
    }
}