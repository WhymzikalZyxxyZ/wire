package xyz.zyxwonderland.wire.producer;

import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * POST /events was found unauthenticated in a security audit — anyone who
 * could reach WIRE could submit arbitrary fabricated transaction events
 * that flow through Kafka and get forwarded to LEDGER as if they came from
 * a legitimate upstream source. This is a lightweight shared-secret check,
 * not a full auth system — appropriate for a service-to-service ingestion
 * point (Spring Security wasn't already a dependency, and pulling it in for
 * one header check would be a much larger surface change than the gap it
 * closes). Declared as a plain @Component: Spring Boot auto-registers any
 * Filter bean it finds, so no separate FilterRegistrationBean is needed —
 * URL scoping to /events is done manually below instead.
 */
@Component
public class ProducerApiKeyFilter implements Filter {

    private static final String HEADER = "X-API-Key";

    private final String expectedKey;

    public ProducerApiKeyFilter(@Value("${wire.producer.api-key}") String expectedKey) {
        this.expectedKey = expectedKey;
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        HttpServletRequest httpRequest = (HttpServletRequest) request;
        HttpServletResponse httpResponse = (HttpServletResponse) response;

        if (!httpRequest.getRequestURI().startsWith("/events")) {
            chain.doFilter(request, response);
            return;
        }

        String provided = httpRequest.getHeader(HEADER);
        if (provided == null || !constantTimeEquals(provided, expectedKey)) {
            httpResponse.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
            httpResponse.setContentType("application/json");
            httpResponse.getWriter().write(
                    "{\"error\":\"unauthorized\",\"message\":\"Missing or invalid " + HEADER + ".\"}");
            return;
        }
        chain.doFilter(request, response);
    }

    private boolean constantTimeEquals(String provided, String expected) {
        return MessageDigest.isEqual(
                provided.getBytes(StandardCharsets.UTF_8), expected.getBytes(StandardCharsets.UTF_8));
    }
}
