package io.kestra.scheduler.events;

import io.kestra.core.models.flows.FlowWithSource;
import io.kestra.core.models.triggers.AbstractTrigger;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.scheduler.internals.NextEvaluationDate;
import io.kestra.scheduler.models.TriggerState;
import io.kestra.scheduler.models.TriggerStatus;
import io.kestra.scheduler.pubsub.TriggerExecutionPublisher;
import io.kestra.scheduler.stores.FlowStateStore;
import io.kestra.scheduler.stores.TriggerStateStore;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.time.Clock;
import java.util.Optional;

/**
 * A service for handling {@link TriggerEvent}.
 */
@Singleton
public class TriggerEventHandler {
    
    private final TriggerStateStore triggerStateStore;
    private final FlowStateStore flowStateStore;
    private final TriggerExecutionPublisher triggerExecutionPublisher;
    
    @Inject
    public TriggerEventHandler(TriggerStateStore triggerStateStore,
                               FlowStateStore flowStateStore,
                               TriggerExecutionPublisher triggerExecutionPublisher) {
        this.triggerStateStore = triggerStateStore;
        this.flowStateStore = flowStateStore;
        this.triggerExecutionPublisher = triggerExecutionPublisher;
    }
    
    public void handle(Clock clock, Integer vNode, TriggerEvent event) {
        switch (event) {
            case TriggerCreated triggerCreated -> {
                onTriggerCreated(triggerCreated, vNode);
            }
            case TriggerDeleted triggerDeleted -> {
                onTriggerDeleted(triggerDeleted);
            }
            case TriggerUpdated triggerUpdated -> {
            }
            case ResetTrigger triggerReset -> {
            }
            case TriggerCompleted triggerCompleted -> {
                onTriggerCompleted(clock, triggerCompleted);
            }
            case TriggerExecuted triggerExecuted -> {
                onTriggerExecuted(clock, triggerExecuted);
            }
            default -> throw new IllegalStateException("Unexpected value: " + event);
        }
    }
    
    public void onTriggerCompleted(Clock clock, TriggerCompleted event) {
        TriggerState newState = triggerStateStore.find(event.id()).orElseThrow();
        newState = newState
            .status(clock, TriggerStatus.IDLE)
            .updateForExecutionState(clock, event.executionState());
        triggerStateStore.save(newState);
    }
    
    public void onTriggerExecuted(Clock clock, TriggerExecuted event) {
        TriggerState triggerState = triggerStateStore.find(event.id()).orElseThrow();// TODO
        
        FlowWithSource flowWithSource = flowStateStore.findFlow(event.id().tenant(), event.id().namespace(), event.id().flowId()).orElseThrow();// TODO
        
        Optional<AbstractTrigger> trigger = flowWithSource.getTriggers().stream()
            .filter(it -> it.getId().equals(event.id().triggerId()))
            .findFirst();
        
        TriggerState newState = triggerState;
        if (trigger.isPresent()) {
            newState = triggerState
                .updateForNextEvaluationDate(clock, NextEvaluationDate.get(clock, trigger.get()));
        }
        
        if (event.execution() != null) {
            newState.updateForExecution(clock, event.execution());
        }
        
        triggerStateStore.save(newState);
        
        if (event.execution() != null) {
            triggerExecutionPublisher.sendExecution(event.execution(), triggerState.context());
        }
    }
    
    public void onTriggerDeleted(TriggerDeleted event) {
        triggerStateStore.delete(event.id());
    }
    
    public void onTriggerCreated(TriggerCreated triggerCreated, Integer vNode) {
        TriggerState newState = TriggerState.of(
            TriggerContext
                .builder()
                .tenantId(triggerCreated.id().tenant())
                .namespace(triggerCreated.id().namespace())
                .flowId(triggerCreated.id().flowId())
                .triggerId(triggerCreated.id().triggerId())
                .vnode(vNode)
                .disabled(triggerCreated.disabled())
                .build()
        );
        triggerStateStore.save(newState);
    }
}
