package com.att.cassandra.autoconfig;

import org.springframework.boot.context.properties.ConfigurationProperties;
import lombok.Data;

/**
 * Configuration properties for Cassandra MFA client in Spring Boot applications.
 *
 * <p>This class binds configuration properties prefixed with {@code cassandra-mfa}
 * from {@code application.yml} or {@code application.properties} files.</p>
 *
 * <h2>Properties:</h2>
 * <table border="1">
 *   <tr><th>Property</th><th>Type</th><th>Default</th><th>Description</th></tr>
 *   <tr><td>tenant-id</td><td>String</td><td>-</td><td>Azure AD tenant ID</td></tr>
 *   <tr><td>client-id</td><td>String</td><td>-</td><td>Azure AD application client ID</td></tr>
 *   <tr><td>client-secret</td><td>String</td><td>-</td><td>Azure AD client secret</td></tr>
 *   <tr><td>scope</td><td>String</td><td>-</td><td>OAuth scope for token request</td></tr>
 *   <tr><td>host</td><td>String</td><td>-</td><td>Cassandra host</td></tr>
 *   <tr><td>port</td><td>int</td><td>9042</td><td>Cassandra CQL port</td></tr>
 *   <tr><td>local-dc</td><td>String</td><td>-</td><td>Local datacenter name</td></tr>
 *   <tr><td>ssl-enabled</td><td>boolean</td><td>false</td><td>Enable SSL/TLS</td></tr>
 *   <tr><td>truststore</td><td>String</td><td>-</td><td>Path to JKS truststore</td></tr>
 *   <tr><td>truststore-password</td><td>String</td><td>-</td><td>Truststore password</td></tr>
 * </table>
 *
 * <h2>Example Configuration (application.yml):</h2>
 * <pre>
 * cassandra-mfa:
 *   tenant-id: ${AZURE_TENANT_ID}
 *   client-id: ${AZURE_CLIENT_ID}
 *   client-secret: ${AZURE_CLIENT_SECRET}
 *   scope: api://my-app/.default
 *   host: cassandra.example.com
 *   port: 9042
 *   local-dc: datacenter1
 *   ssl-enabled: true
 *   truststore: /etc/ssl/cassandra-truststore.jks
 *   truststore-password: ${TRUSTSTORE_PASSWORD}
 * </pre>
 *
 * @see CassandraMfaAutoConfiguration
 */
@Data
@ConfigurationProperties(prefix = "cassandra-mfa")
public class CassandraMfaProperties {
    private String tenantId;
    private String clientId;
    private String clientSecret;
    private String scope;
    private String host;
    private int port = 9042;
    private String localDc;
    private boolean sslEnabled = false;
    private String truststore;
    private String truststorePassword;
}