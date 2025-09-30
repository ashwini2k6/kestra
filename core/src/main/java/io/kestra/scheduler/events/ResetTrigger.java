package io.kestra.scheduler.events;

import io.kestra.scheduler.models.TriggerId;

import java.time.Instant;

/**
 * A command to reset a trigger.
 */
public record ResetTrigger(
    TriggerId id,
    Instant timestamp
) implements TriggerEvent {

}
