package com.att.cassandra.client;

import com.datastax.oss.driver.api.core.auth.AuthProvider;
import com.datastax.oss.driver.api.core.auth.AuthenticationException;
import com.datastax.oss.driver.api.core.auth.Authenticator;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import edu.umd.cs.findbugs.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class AzureAdAuthProvider implements AuthProvider {

    private static final Logger log = LoggerFactory.getLogger(AzureAdAuthProvider.class);

    private final AzureAdTokenProvider tokenProvider;

    public AzureAdAuthProvider(AzureAdTokenProvider tokenProvider) {
        this.tokenProvider = tokenProvider;
        log.info("AzureAdAuthProvider initialized");
    }

    @NonNull
    @Override
    public Authenticator newAuthenticator(@NonNull EndPoint endPoint, @NonNull String serverAuthenticator)
            throws AuthenticationException {
        log.debug("Creating new authenticator for endpoint: {}, server authenticator: {}", endPoint, serverAuthenticator);
        return new JwtTokenAuthenticator(tokenProvider);
    }

    @Override
    public void onMissingChallenge(@NonNull EndPoint endPoint) throws AuthenticationException {
        log.debug("onMissingChallenge called for endpoint: {} (no-op)", endPoint);
    }

    @Override
    public void close() {
        log.debug("AzureAdAuthProvider closed");
    }

    public static class JwtTokenAuthenticator implements Authenticator {

        private static final Logger log = LoggerFactory.getLogger(JwtTokenAuthenticator.class);

        private final AzureAdTokenProvider tokenProvider;

        public JwtTokenAuthenticator(AzureAdTokenProvider tokenProvider) {
            this.tokenProvider = tokenProvider;
            log.trace("JwtTokenAuthenticator created");
        }

        @Override
        public CompletionStage<ByteBuffer> initialResponse() {
            log.debug("Preparing initial authentication response with JWT token");
            try {
                String username = "";
                String password = tokenProvider.getToken();  // JWT

                byte[] u = username.getBytes();
                byte[] p = password.getBytes();

                ByteBuffer buffer = ByteBuffer.allocate(u.length + 1 + p.length);
                buffer.put(u);
                buffer.put((byte) 0);  // username/password separator
                buffer.put(p);
                buffer.flip();

                log.debug("Initial authentication response prepared successfully (token length: {} bytes)", p.length);
                return CompletableFuture.completedFuture(buffer);
            } catch (Exception e) {
                log.error("Failed to prepare initial authentication response", e);
                throw e;
            }
        }

        @Override
        public CompletionStage<ByteBuffer> evaluateChallenge(ByteBuffer challenge) {
            log.debug("evaluateChallenge called (returning null - no SASL challenge expected)");
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletionStage<Void> onAuthenticationSuccess(ByteBuffer token) {
            log.info("Authentication successful");
            return CompletableFuture.completedFuture(null);
        }
    }
}