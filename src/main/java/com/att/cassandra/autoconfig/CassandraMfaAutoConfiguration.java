package com.att.cassandra.autoconfig;

import com.att.cassandra.client.AzureAdAuthProvider;
import com.att.cassandra.client.AzureAdTokenProvider;
import com.att.cassandra.client.SslUtil;
import com.datastax.oss.driver.api.core.CqlSession;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.InetSocketAddress;

@Configuration
@EnableConfigurationProperties(CassandraMfaProperties.class)
public class CassandraMfaAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public AzureAdTokenProvider azureAdTokenProvider(CassandraMfaProperties props) {
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
        return new AzureAdAuthProvider(tokenProvider);
    }

    @Bean
    @ConditionalOnMissingBean
    public CqlSession cassandraMfaSession(CassandraMfaProperties props,
                                          AzureAdAuthProvider authProvider) throws Exception {

        return CqlSession.builder()
                .addContactPoint(new InetSocketAddress(props.getHost(), props.getPort()))
                .withLocalDatacenter(props.getLocalDc())
                .withAuthProvider(authProvider)
                .withSslContext(SslUtil.createSslContext(
                        props.getTruststore(), props.getTruststorePassword()))
                .build();
    }
}