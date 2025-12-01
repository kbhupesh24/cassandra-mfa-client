package com.att.cassandra.client;

import com.azure.core.credential.AccessToken;
import com.azure.core.credential.TokenRequestContext;
import com.azure.identity.ClientSecretCredential;
import com.azure.identity.ClientSecretCredentialBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.OffsetDateTime;

public class AzureAdTokenProvider {

    private static final Logger log = LoggerFactory.getLogger(AzureAdTokenProvider.class);

    // How many seconds before expiry we proactively refresh
    private static final Duration SKEW = Duration.ofMinutes(2);
    private static final int MAX_RETRIES = 3;
    private static final long RETRY_BACKOFF_MS = 1000L;

    private final ClientSecretCredential credential;
    private final String scope;

    // cached token
    private volatile AccessToken cachedToken;

    public AzureAdTokenProvider(String tenantId,
                                String clientId,
                                String clientSecret,
                                String scope) {

        this.credential = new ClientSecretCredentialBuilder()
                .tenantId(tenantId)
                .clientId(clientId)
                .clientSecret(clientSecret)
                .build();

        this.scope = scope;
    }

    /**
     * Thread-safe token retrieval with caching and proactive refresh.
     */
    public String getToken() {
        try {
            AccessToken token = cachedToken;

            if (token == null || isExpiringSoon(token)) {
                synchronized (this) {
                    // double-check inside lock
                    token = cachedToken;
                    if (token == null || isExpiringSoon(token)) {
                        cachedToken = token = fetchTokenWithRetry();
                    }
                }
            }

            return token.getToken();
        } catch (RuntimeException e) {
            log.error("Failed to acquire Azure AD token", e);
            throw e;
        }
    }

    private boolean isExpiringSoon(AccessToken token) {
        OffsetDateTime expiresAt = token.getExpiresAt();
        if (expiresAt == null) {
            return true;
        }
        OffsetDateTime now = OffsetDateTime.now();
        return expiresAt.minus(SKEW).isBefore(now);
    }

    private AccessToken fetchTokenWithRetry() {
        TokenRequestContext ctx = new TokenRequestContext().addScopes(scope);
        RuntimeException lastException = null;

        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            try {
                log.info("Requesting Azure AD token (attempt {}/{})", attempt, MAX_RETRIES);
                AccessToken token = credential.getToken(ctx).block();
                if (token == null) {
                    throw new IllegalStateException("Received null AccessToken from Azure Identity");
                }
                log.info("Received Azure AD token expiring at {}", token.getExpiresAt());
                return token;
            } catch (RuntimeException e) {
                lastException = e;
                log.warn("Error requesting Azure AD token (attempt {}): {}", attempt, e.toString());
                if (attempt < MAX_RETRIES) {
                    try {
                        Thread.sleep(RETRY_BACKOFF_MS * attempt);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Interrupted while retrying token fetch", ie);
                    }
                }
            }
        }

        throw new RuntimeException("Failed to obtain Azure AD token after " + MAX_RETRIES + " attempts", lastException);
    }
}