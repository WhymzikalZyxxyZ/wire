package xyz.zyxwonderland.wire.producer;

import java.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class ApiExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(ApiExceptionHandler.class);

    public record ErrorResponse(String error, String message, Instant timestamp) {
        static ErrorResponse of(String error, String message) {
            return new ErrorResponse(error, message, Instant.now());
        }
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ErrorResponse> handleValidation(MethodArgumentNotValidException e) {
        String message = e.getBindingResult().getFieldErrors().stream()
                .map(fe -> fe.getField() + ": " + fe.getDefaultMessage())
                .reduce((a, b) -> a + "; " + b)
                .orElse("Validation failed");
        return ResponseEntity.badRequest().body(ErrorResponse.of("validation_failed", message));
    }

    /**
     * Found missing in a post-implementation code survey: EventProducer's
     * publish failure (the broker unreachable, or a send that never got
     * acknowledged in time) previously had no handler here, so it fell
     * through to Spring Boot's default, unstructured error response. 503,
     * not 500 — the request itself was fine; the broker WIRE depends on
     * wasn't reachable, which is a meaningfully different, retryable
     * condition for the caller.
     */
    @ExceptionHandler(EventPublishException.class)
    public ResponseEntity<ErrorResponse> handlePublishFailure(EventPublishException e) {
        log.error("failed to publish an incoming event", e);
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .body(ErrorResponse.of("publish_failed", "Could not queue the event — try again shortly."));
    }

    /**
     * Catch-all so an unexpected failure still returns this API's
     * consistent ErrorResponse shape instead of leaking implementation
     * details (stack traces, exception class names) to the client. The
     * full exception is logged server-side.
     */
    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> handleUnexpected(Exception e) {
        log.error("unhandled exception", e);
        return ResponseEntity.internalServerError()
                .body(ErrorResponse.of("internal_error", "An unexpected error occurred."));
    }
}
