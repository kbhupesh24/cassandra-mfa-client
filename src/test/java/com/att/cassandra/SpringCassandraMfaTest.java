package com.att.cassandra;

import com.att.cassandra.autoconfig.CassandraMfaAutoConfiguration;
import com.datastax.oss.driver.api.core.CqlSession;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Import;

/**
 * Tests Cassandra MFA connection using Spring Boot auto-configuration.
 *
 * Run with: -Dspring.profiles.active=local
 */
@SpringBootApplication(scanBasePackages = "com.att.cassandra")
@Import(CassandraMfaAutoConfiguration.class)
public class SpringCassandraMfaTest {

    public static void main(String[] args) {
        // Hint: Run with VM option -Dspring.profiles.active=local
        try (ConfigurableApplicationContext context = SpringApplication.run(SpringCassandraMfaTest.class, args)) {

            // Get the auto-configured CqlSession bean
            CqlSession session = context.getBean(CqlSession.class);

            System.out.println("Spring Cassandra MFA connection established!");

            // Execute test query
            session.execute("SELECT release_version FROM system.local")
                    .forEach(row ->
                        System.out.println("Cassandra version = " + row.getString("release_version")));

            System.out.println("Test completed successfully!");
        }
    }
}
