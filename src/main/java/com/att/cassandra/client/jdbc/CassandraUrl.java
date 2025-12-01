package com.att.cassandra.client.jdbc;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public final class CassandraUrl {

    public final String host;
    public final int port;
    public final String localDc;

    public final String tenantId;
    public final String clientId;
    public final String clientSecret;
    public final String scope;

    public final String truststore;
    public final String truststorePassword;

    private CassandraUrl(String host,
                         int port,
                         String localDc,
                         String tenantId,
                         String clientId,
                         String clientSecret,
                         String scope,
                         String truststore,
                         String truststorePassword) {

        this.host = host;
        this.port = port;
        this.localDc = localDc;
        this.tenantId = tenantId;
        this.clientId = clientId;
        this.clientSecret = clientSecret;
        this.scope = scope;
        this.truststore = truststore;
        this.truststorePassword = truststorePassword;
    }

    /**
     * Example URL:
     *
     * jdbc:cassandra-mfa://localhost:9042/DC1
     *   ?tenantId=...&clientId=...&clientSecret=...&scope=...&
     *    truststore=/path/to/truststore.jks&truststorePassword=changeit
     */
    public static CassandraUrl parse(String url, Properties info) throws SQLException {
        if (url == null || !url.startsWith("jdbc:cassandra-mfa://")) {
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
                throw new SQLException("Invalid port in Cassandra URL: " + portStr, e);
            }
        } else {
            host = hostPortPart;
        }

        Map<String, String> params = parseQuery(queryPart);
        // Fallbacks to Properties if not present in URL
        String tenantId = get(params, info, "tenantId");
        String clientId = get(params, info, "clientId");
        String clientSecret = get(params, info, "clientSecret");
        String scope = get(params, info, "scope");

        String truststore = get(params, info, "truststore");
        String truststorePassword = get(params, info, "truststorePassword");

        if (tenantId == null || clientId == null || clientSecret == null || scope == null) {
            throw new SQLException("Missing required Azure AD parameters in URL or properties (tenantId, clientId, clientSecret, scope)");
        }

        String localDc = (dcPart != null && !dcPart.isEmpty()) ? dcPart : get(params, info, "localDc");
        if (localDc == null) {
            throw new SQLException("Missing localDc in Cassandra URL or properties");
        }

        if (truststore == null || truststorePassword == null) {
            throw new SQLException("Missing truststore or truststorePassword in URL or properties");
        }

        return new CassandraUrl(
                host,
                port,
                localDc,
                tenantId,
                clientId,
                clientSecret,
                scope,
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