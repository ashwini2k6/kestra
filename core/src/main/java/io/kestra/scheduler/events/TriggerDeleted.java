package io.kestra.scheduler.events;

import io.kestra.scheduler.models.TriggerId;

import java.time.Instant;

/**
 * An existing trigger was deleted (i.e. removed from a flow).
 */
public record TriggerDeleted(
    TriggerId id,
    Instant timestamp
) implements TriggerEvent {

}
