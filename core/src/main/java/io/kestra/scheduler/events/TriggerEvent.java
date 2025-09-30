package io.kestra.scheduler.events;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.kestra.core.models.HasUID;
import io.kestra.scheduler.models.TriggerId;

import java.time.Instant;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "type", visible = true)
@JsonSubTypes({
    @JsonSubTypes.Type(value = TriggerCreated.class, name = "TRIGGER_CREATED"),
    @JsonSubTypes.Type(value = TriggerUpdated.class, name = "TRIGGER_UPDATED"),
    @JsonSubTypes.Type(value = TriggerDeleted.class, name = "TRIGGER_DELETED"),
    @JsonSubTypes.Type(value = TriggerExecuted.class, name = "TRIGGER_EXECUTED"),
    @JsonSubTypes.Type(value = BackfillTrigger.class, name = "BACKFILL_TRIGGER"),
    @JsonSubTypes.Type(value = ResetTrigger.class, name = "RESET_TRIGGER"),
    @JsonSubTypes.Type(value = DisableTrigger.class, name = "DISABLE_TRIGGER"),
})
public interface TriggerEvent extends HasUID {
    
    /**
     * @return the trigger identifier.
     */
    TriggerId id();
    
    /**
     * @return the event timestamp.
     */
    Instant timestamp();
    
    /**
     * @return the event type.
     */
    default Type type() {
        return Type.from(this.getClass());
    }
    
    @Override
    default String uid() {
        return this.id().uid();
    }
    
}
