package xyz.zyxwonderland.wire.producer;

import jakarta.validation.Valid;
import java.time.Instant;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import xyz.zyxwonderland.wire.event.WireTransactionEvent;

/**
 * WIRE's synthetic event source — see docs/RISKS.md's "no real upstream
 * event source" entry. A real deployment would have this be a Kafka
 * Connect source connector or an SDK call from an upstream system;
 * exposing it as a REST endpoint keeps the producer side demonstrable and
 * testable without inventing a fake external integration.
 */
@RestController
@RequestMapping("/events")
public class EventController {

    private final EventProducer eventProducer;

    public EventController(EventProducer eventProducer) {
        this.eventProducer = eventProducer;
    }

    @PostMapping
    public ResponseEntity<Void> publish(@Valid @RequestBody WireTransactionEvent event) {
        if (event.occurredAt() == null) {
            event = new WireTransactionEvent(event.eventId(), event.description(), event.entries(), Instant.now());
        }
        eventProducer.publish(event);
        return ResponseEntity.accepted().build();
    }
}
