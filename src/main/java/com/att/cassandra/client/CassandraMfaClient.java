package com.att.cassandra.client;

import com.datastax.oss.driver.api.core.CqlSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

public class CassandraMfaClient {

    private static final Logger log = LoggerFactory.getLogger(CassandraMfaClient.class);

    public static void main(String[] args) {
        log.info("Starting Cassandra MFA Client");

        try {
            String host = Config.get("cassandra.host");
            int port = Integer.parseInt(Config.get("cassandra.port"));
            String datacenter = Config.get("cassandra.dc");

            log.info("Connecting to Cassandra at {}:{} in datacenter '{}'", host, port, datacenter);

            log.debug("Initializing Azure AD token provider");
            AzureAdTokenProvider tokenProvider =
                    new AzureAdTokenProvider(
                            Config.get("azure.tenant-id"),
                            Config.get("azure.client-id"),
                            Config.get("azure.client-secret"),
                            Config.get("azure.scope")
                    );

            log.debug("Building CqlSession with MFA authentication");
            CqlSession session = CqlSession.builder()
                    .addContactPoint(new InetSocketAddress(host, port))
                    .withLocalDatacenter(datacenter)
                    .withAuthProvider(new AzureAdAuthProvider(tokenProvider))
                    .withSslContext(
                            SslUtil.createSslContext(
                                    Config.get("cassandra.truststore"),
                                    Config.get("cassandra.truststore-password")
                            )
                    )
                    .build();

            log.info("Successfully connected to Cassandra with MFA authentication");

            log.debug("Executing test query: SELECT release_version FROM system.local");
            session.execute("SELECT release_version FROM system.local")
                    .forEach(row -> {
                        String version = row.getString("release_version");
                        log.info("Cassandra version: {}", version);
                    });

            log.debug("Closing Cassandra session");
            session.close();
            log.info("Cassandra MFA Client completed successfully");

        } catch (NumberFormatException e) {
            log.error("Invalid port number in configuration", e);
            System.exit(1);
        } catch (Exception e) {
            log.error("Failed to connect to Cassandra", e);
            System.exit(1);
        }
    }
}