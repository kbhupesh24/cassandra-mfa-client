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
        log.info("Cassandra connection: {}:{}, datacenter: {}", props.getHost(), props.getPort(), props.getLocalDc());
        log.debug("SSL truststore: {}", props.getTruststore());

        try {
            CqlSession session = CqlSession.builder()
                    .addContactPoint(new InetSocketAddress(props.getHost(), props.getPort()))
                    .withLocalDatacenter(props.getLocalDc())
                    .withAuthProvider(authProvider)
                    .withSslContext(SslUtil.createSslContext(
                            props.getTruststore(), props.getTruststorePassword()))
                    .build();

            log.info("CqlSession created successfully");
            return session;
        } catch (Exception e) {
            log.error("Failed to create CqlSession", e);
            throw e;
        }
    }
}