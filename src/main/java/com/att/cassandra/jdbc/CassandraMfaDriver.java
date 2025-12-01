package com.att.cassandra.jdbc;

import com.att.cassandra.client.AzureAdAuthProvider;
import com.att.cassandra.client.AzureAdTokenProvider;
import com.att.cassandra.client.SslUtil;
import com.att.cassandra.client.jdbc.CassandraMfaConnection;
import com.att.cassandra.client.jdbc.CassandraUrl;
import com.datastax.oss.driver.api.core.CqlSession;

import java.net.InetSocketAddress;
import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

public class CassandraMfaDriver implements Driver {

    static {
        try {
            DriverManager.registerDriver(new CassandraMfaDriver());
        } catch (SQLException e) {
            throw new RuntimeException("Failed to register CassandraMfaDriver", e);
        }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }

        CassandraUrl parsed = CassandraUrl.parse(url, info);

        AzureAdTokenProvider tokenProvider = new AzureAdTokenProvider(
                parsed.tenantId,
                parsed.clientId,
                parsed.clientSecret,
                parsed.scope
        );

        try {
            CqlSession session = CqlSession.builder()
                    .addContactPoint(new InetSocketAddress(parsed.host, parsed.port))
                    .withLocalDatacenter(parsed.localDc)
                    .withAuthProvider(new AzureAdAuthProvider(tokenProvider))
                    .withSslContext(SslUtil.createSslContext(parsed.truststore, parsed.truststorePassword))
                    .build();

            return new CassandraMfaConnection(session);
        } catch (Exception e) {
            throw new SQLException("Unable to create Cassandra MFA connection", e);
        }
    }

    @Override
    public boolean acceptsURL(String url) {
        return url != null && url.startsWith("jdbc:cassandra-mfa://");
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
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
    public Logger getParentLogger() {
        return Logger.getLogger("CassandraMfaDriver");
    }
}