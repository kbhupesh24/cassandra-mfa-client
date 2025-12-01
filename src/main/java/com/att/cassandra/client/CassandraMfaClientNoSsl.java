package com.att.cassandra.client;

import com.datastax.oss.driver.api.core.CqlSession;

import java.net.InetSocketAddress;

/**
 * client_encryption_options:
 *   enabled: false
 */
public class CassandraMfaClientNoSsl {

    public static void main(String[] args) {

        // For quick testing you can hardcode here, or read from env / properties
        String tenantId = System.getenv("AZURE_TENANT_ID");
        String clientId = System.getenv("AZURE_CLIENT_ID");
        String clientSecret = System.getenv("AZURE_CLIENT_SECRET");
        String scope = "https://cassandra.azure.com/.default"; // or whatever your cluster expects

        String host = "localhost";   // your Cassandra node
        int port = 9042;             // default CQL port
        String localDc = "DC1";      // must match your cluster DC name

        AzureAdTokenProvider tokenProvider =
                new AzureAdTokenProvider(tenantId, clientId, clientSecret, scope);

        AzureAdAuthProvider authProvider = new AzureAdAuthProvider(tokenProvider);

        // NO SSL HERE → plain CQL over TCP
        try (CqlSession session = CqlSession.builder()
                .addContactPoint(new InetSocketAddress(host, port))
                .withLocalDatacenter(localDc)
                .withAuthProvider(authProvider)
                .build()) {

            System.out.println("Connected to Cassandra (NO SSL) with MFA!");

            session.execute("SELECT release_version FROM system.local")
                    .forEach(row ->
                            System.out.println("Cassandra version = " + row.getString("release_version")));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}