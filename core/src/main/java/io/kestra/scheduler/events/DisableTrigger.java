package io.kestra.scheduler.events;

import io.kestra.scheduler.models.TriggerId;

import java.time.Instant;

/**
 * A command to disable/enable a trigger.
 */
public record DisableTrigger(
    TriggerId id,
    Instant timestamp,
    Boolean disabled
) implements TriggerEvent {
    
}
