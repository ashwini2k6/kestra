package io.kestra.scheduler.events;

import io.kestra.scheduler.models.TriggerId;

import java.time.Instant;

/**
 * A new trigger was created (i.e. added to a flow).
 */
public record TriggerCreated(
    TriggerId id,
    Instant timestamp,
    Boolean disabled
) implements TriggerEvent {
    
}
