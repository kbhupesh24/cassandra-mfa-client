package com.att.cassandra.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;

/**
 * Utility class for creating SSL/TLS contexts for secure Cassandra connections.
 *
 * <p>This class provides methods to create {@link SSLContext} instances from
 * JKS (Java KeyStore) truststore files. The created context can be used with
 * the DataStax Java Driver to establish encrypted connections to Cassandra.</p>
 *
 * <h2>Usage:</h2>
 * <pre>{@code
 * SSLContext sslContext = SslUtil.createSslContext(
 *     "/path/to/truststore.jks",
 *     "truststorePassword"
 * );
 *
 * CqlSession session = CqlSession.builder()
 *     .addContactPoint(new InetSocketAddress("cassandra.example.com", 9042))
 *     .withSslContext(sslContext)
 *     .withAuthProvider(authProvider)
 *     .build();
 * }</pre>
 *
 * <h2>Truststore Requirements:</h2>
 * <ul>
 *   <li>Format: JKS (Java KeyStore)</li>
 *   <li>Must contain the CA certificate(s) that signed the Cassandra server certificate</li>
 *   <li>For self-signed certificates, import the server's certificate directly</li>
 * </ul>
 *
 * <h2>Creating a Truststore:</h2>
 * <pre>
 * # Import a CA certificate into a new truststore
 * keytool -import -trustcacerts -alias cassandra-ca \
 *     -file ca-cert.pem -keystore truststore.jks -storepass changeit
 * </pre>
 *
 * @see javax.net.ssl.SSLContext
 */
public class SslUtil {

    private static final Logger log = LoggerFactory.getLogger(SslUtil.class);

    public static SSLContext createSslContext(String truststorePath, String truststorePassword) throws Exception {
        log.info("Creating SSL context with truststore: {}", truststorePath);

        try {
            log.debug("Loading JKS keystore from: {}", truststorePath);
            KeyStore ks = KeyStore.getInstance("JKS");
            try (FileInputStream fis = new FileInputStream(truststorePath)) {
                ks.load(fis, truststorePassword.toCharArray());
            }
            log.debug("Keystore loaded successfully, contains {} entries", ks.size());

            log.debug("Initializing TrustManagerFactory with SunX509 algorithm");
            TrustManagerFactory tmf = TrustManagerFactory.getInstance("SunX509");
            tmf.init(ks);
            log.debug("TrustManagerFactory initialized with {} trust managers", tmf.getTrustManagers().length);

            log.debug("Creating TLS SSLContext for Cassandra MFA");
            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, tmf.getTrustManagers(), null);

            log.info("SSL context created successfully (protocol: {})", sslContext.getProtocol());
            return sslContext;

        } catch (KeyStoreException e) {
            log.error("Failed to create keystore instance", e);
            throw e;
        } catch (IOException e) {
            log.error("Failed to load truststore from path: {}", truststorePath, e);
            throw e;
        } catch (NoSuchAlgorithmException e) {
            log.error("TLS or SunX509 algorithm not available", e);
            throw e;
        } catch (CertificateException e) {
            log.error("Failed to load certificates from truststore", e);
            throw e;
        }
    }
}