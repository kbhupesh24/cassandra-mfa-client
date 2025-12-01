package com.att.cassandra.client;

import com.datastax.oss.driver.api.core.CqlSession;

import java.net.InetSocketAddress;

public class CassandraMfaClient {

    public static void main(String[] args) throws Exception {

        AzureAdTokenProvider tokenProvider =
                new AzureAdTokenProvider(
                        Config.get("azure.tenant-id"),
                        Config.get("azure.client-id"),
                        Config.get("azure.client-secret"),
                        Config.get("azure.scope")
                );

        CqlSession session = CqlSession.builder()
                .addContactPoint(new InetSocketAddress(
                        Config.get("cassandra.host"),
                        Integer.parseInt(Config.get("cassandra.port"))
                ))
                .withLocalDatacenter(Config.get("cassandra.dc"))
                .withAuthProvider(new AzureAdAuthProvider(tokenProvider))
                .withSslContext(
                        SslUtil.createSslContext(
                                Config.get("cassandra.truststore"),
                                Config.get("cassandra.truststore-password")
                        )
                )
                .withAuthProvider(new AzureAdAuthProvider(tokenProvider))
                .build();

        System.out.println("Connected to Cassandra with MFA!");

        session.execute("SELECT release_version FROM system.local")
                .forEach(row -> System.out.println("Version = " + row.getString("release_version")));

        session.close();
    }
}