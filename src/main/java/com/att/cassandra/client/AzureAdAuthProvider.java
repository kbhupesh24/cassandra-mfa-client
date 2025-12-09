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

/**
 * DataStax Java Driver authentication provider that uses Azure AD JWT tokens.
 *
 * <p>This class implements the {@link AuthProvider} interface to enable JWT-based
 * authentication with Cassandra clusters configured with the MFA security plugin.
 * It works in conjunction with {@link AzureAdTokenProvider} to obtain and send
 * JWT tokens using the SASL PLAIN mechanism.</p>
 *
 * <h2>Authentication Flow:</h2>
 * <ol>
 *   <li>Client connects to Cassandra and server requests authentication</li>
 *   <li>This provider creates a {@link JwtTokenAuthenticator}</li>
 *   <li>The authenticator obtains a JWT token from {@link AzureAdTokenProvider}</li>
 *   <li>Token is sent as "Bearer {token}" in SASL PLAIN format</li>
 *   <li>Server's JwtAuthenticator validates the token signature and claims</li>
 * </ol>
 *
 * <h2>SASL PLAIN Format:</h2>
 * <p>The authentication payload follows RFC 4616 SASL PLAIN format:</p>
 * <pre>
 * [authzid]\0[username]\0[password]
 * </pre>
 * <p>Where password is "Bearer {jwt-token}"</p>
 *
 * <h2>Usage with CqlSession:</h2>
 * <pre>{@code
 * AzureAdTokenProvider tokenProvider = new AzureAdTokenProvider(...);
 * AzureAdAuthProvider authProvider = new AzureAdAuthProvider(tokenProvider);
 *
 * CqlSession session = CqlSession.builder()
 *     .addContactPoint(new InetSocketAddress("localhost", 9042))
 *     .withLocalDatacenter("datacenter1")
 *     .withAuthProvider(authProvider)
 *     .build();
 * }</pre>
 *
 * @see AzureAdTokenProvider
 * @see com.datastax.oss.driver.api.core.auth.AuthProvider
 */
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
                String authzid = "";  // authorization identity (empty)
                String username = "";  // authentication identity (empty for JWT)
                String password = "Bearer " + tokenProvider.getToken();  // JWT with Bearer prefix

                byte[] a = authzid.getBytes();
                byte[] u = username.getBytes();
                byte[] p = password.getBytes();

                // SASL PLAIN format: [authzid]\0[username]\0[password]
                ByteBuffer buffer = ByteBuffer.allocate(a.length + 1 + u.length + 1 + p.length);
                buffer.put(a);
                buffer.put((byte) 0);  // authzid/username separator
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