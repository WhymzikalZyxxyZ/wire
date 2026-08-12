package xyz.zyxwonderland.wire.producer;

/** The broker couldn't be reached to accept an incoming event — see EventController's handling. */
public class EventPublishException extends RuntimeException {

    public EventPublishException(String message, Throwable cause) {
        super(message, cause);
    }
}
