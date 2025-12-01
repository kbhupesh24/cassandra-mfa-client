package com.att.cassandra.client;

import com.datastax.oss.driver.api.core.auth.AuthProvider;
import com.datastax.oss.driver.api.core.auth.AuthenticationException;
import com.datastax.oss.driver.api.core.auth.Authenticator;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import edu.umd.cs.findbugs.annotations.NonNull;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class AzureAdAuthProvider implements AuthProvider {

    private final AzureAdTokenProvider tokenProvider;

    public AzureAdAuthProvider(AzureAdTokenProvider tokenProvider) {
        this.tokenProvider = tokenProvider;
    }

    @NonNull
    @Override
    public Authenticator newAuthenticator(@NonNull EndPoint endPoint, @NonNull String s)
            throws AuthenticationException {
        return new JwtTokenAuthenticator(tokenProvider);
    }

    @Override
    public void onMissingChallenge(@NonNull EndPoint endPoint) throws AuthenticationException {
        // no-op; your JwtAuthenticator does not require SASL challenge
    }

    @Override
    public void close() {
        // no-op
    }

    public static class JwtTokenAuthenticator implements Authenticator {

        private final AzureAdTokenProvider tokenProvider;

        public JwtTokenAuthenticator(AzureAdTokenProvider tokenProvider) {
            this.tokenProvider = tokenProvider;
        }

        @Override
        public CompletionStage<ByteBuffer> initialResponse() {
            String username = "";
            String password = tokenProvider.getToken();  // JWT

            byte[] u = username.getBytes();
            byte[] p = password.getBytes();

            ByteBuffer buffer = ByteBuffer.allocate(u.length + 1 + p.length);
            buffer.put(u);
            buffer.put((byte) 0);  // username/password separator
            buffer.put(p);
            buffer.flip();

            return CompletableFuture.completedFuture(buffer);
        }

        @Override
        public CompletionStage<ByteBuffer> evaluateChallenge(ByteBuffer challenge) {
            // your server authenticator never sends a challenge; return null or empty
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletionStage<Void> onAuthenticationSuccess(ByteBuffer token) {
            return CompletableFuture.completedFuture(null);
        }
    }
}