package com.att.cassandra.autoconfig;

import com.att.cassandra.client.AzureAdAuthProvider;
import com.att.cassandra.client.AzureAdTokenProvider;
import com.att.cassandra.client.SslUtil;
import com.datastax.oss.driver.api.core.CqlSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.InetSocketAddress;

/**
 * Spring Boot auto-configuration for Cassandra MFA client.
 *
 * <p>This class automatically configures the necessary beans for connecting to
 * Cassandra with Azure AD JWT authentication when included in a Spring Boot application.
 * It creates and wires together the token provider, auth provider, and CqlSession.</p>
 *
 * <h2>Auto-configured Beans:</h2>
 * <ul>
 *   <li>{@link AzureAdTokenProvider} - Obtains JWT tokens from Azure AD</li>
 *   <li>{@link AzureAdAuthProvider} - Provides tokens to the DataStax driver</li>
 *   <li>{@link CqlSession} - Configured Cassandra session with MFA auth</li>
 * </ul>
 *
 * <h2>Configuration Properties:</h2>
 * <p>Configure via {@code application.yml} or {@code application.properties}:</p>
 * <pre>
 * cassandra-mfa:
 *   tenant-id: your-azure-tenant-id
 *   client-id: your-azure-client-id
 *   client-secret: your-azure-client-secret
 *   scope: api://your-app-id/.default
 *   host: localhost
 *   port: 9042
 *   local-dc: datacenter1
 *   ssl-enabled: false
 *   truststore: /path/to/truststore.jks
 *   truststore-password: changeit
 * </pre>
 *
 * <h2>Usage:</h2>
 * <p>Simply inject the CqlSession into your Spring components:</p>
 * <pre>{@code
 * @Service
 * public class MyService {
 *     private final CqlSession session;
 *
 *     public MyService(CqlSession session) {
 *         this.session = session;
 *     }
 *
 *     public void doSomething() {
 *         session.execute("SELECT * FROM keyspace.table");
 *     }
 * }
 * }</pre>
 *
 * @see CassandraMfaProperties
 * @see AzureAdAuthProvider
 */
@Configuration
@EnableConfigurationProperties(CassandraMfaProperties.class)
public class CassandraMfaAutoConfiguration {

    private static final Logger log = LoggerFactory.getLogger(CassandraMfaAutoConfiguration.class);

    @Bean
    @ConditionalOnMissingBean
    public AzureAdTokenProvider azureAdTokenProvider(CassandraMfaProperties props) {
        log.info("Creating AzureAdTokenProvider bean for tenant: {}", props.getTenantId());
        log.debug("Azure AD configuration - clientId: {}, scope: {}", props.getClientId(), props.getScope());
        return new AzureAdTokenProvider(
                props.getTenantId(),
                props.getClientId(),
                props.getClientSecret(),
                props.getScope()
        );
    }

    @Bean
    @ConditionalOnMissingBean
    public AzureAdAuthProvider azureAdAuthProvider(AzureAdTokenProvider tokenProvider) {
        log.info("Creating AzureAdAuthProvider bean");
        return new AzureAdAuthProvider(tokenProvider);
    }

    @Bean
    @ConditionalOnMissingBean
    public CqlSession cassandraMfaSession(CassandraMfaProperties props,
                                          AzureAdAuthProvider authProvider) throws Exception {
        log.info("Creating CqlSession bean with MFA authentication");
        log.info("Cassandra connection: {}:{}, datacenter: {}, ssl: {}",
                props.getHost(), props.getPort(), props.getLocalDc(), props.isSslEnabled());

        try {
            var builder = CqlSession.builder()
                    .addContactPoint(new InetSocketAddress(props.getHost(), props.getPort()))
                    .withLocalDatacenter(props.getLocalDc())
                    .withAuthProvider(authProvider);

            if (props.isSslEnabled()) {
                log.debug("SSL enabled, truststore: {}", props.getTruststore());
                builder.withSslContext(SslUtil.createSslContext(
                        props.getTruststore(), props.getTruststorePassword()));
            }

            CqlSession session = builder.build();
            log.info("CqlSession created successfully");
            return session;
        } catch (Exception e) {
            log.error("Failed to create CqlSession", e);
            throw e;
        }
    }
}