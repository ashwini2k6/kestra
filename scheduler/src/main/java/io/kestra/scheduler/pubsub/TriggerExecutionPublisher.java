package io.kestra.scheduler.pubsub;

import io.kestra.core.events.CrudEvent;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.executions.LogEntry;
import io.kestra.core.models.flows.State;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.queues.QueueException;
import io.kestra.core.queues.QueueInterface;
import io.micronaut.context.event.ApplicationEventPublisher;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class TriggerExecutionPublisher {
    
    private static final Logger LOG = LoggerFactory.getLogger(TriggerExecutionPublisher.class);
    
    private final ApplicationEventPublisher<CrudEvent<Execution>> executionEventPublisher;
    
    // Queues
    private final QueueInterface<Execution> executionQueue;
    private final QueueInterface<LogEntry> logQueue;
    
    @Inject
    public TriggerExecutionPublisher(ApplicationEventPublisher<CrudEvent<Execution>> executionEventPublisher,
                                     QueueInterface<Execution> executionQueue,
                                     QueueInterface<LogEntry> logQueue) {
        this.executionEventPublisher = executionEventPublisher;
        this.executionQueue = executionQueue;
        this.logQueue = logQueue;
    }
    
    public void sendExecution(final Execution execution, final TriggerContext trigger) {
        // we need to be sure that the tenantId is propagated from the trigger to the execution
        Execution newExecution = execution.withTenantId(trigger.getTenantId());
        try {
            this.executionQueue.emit(newExecution);
            this.executionEventPublisher.publishEvent(CrudEvent.create(newExecution));
        } catch (QueueException e) {
            try {
                Execution failedExecution = fail(newExecution, e);
                this.executionQueue.emit(failedExecution);
                this.executionEventPublisher.publishEvent(CrudEvent.create(newExecution));
            } catch (QueueException ex) {
                LOG.error("Unable to emit the execution", ex);
            }
        }
    }
    
    private Execution fail(Execution message, Exception e) {
        var failedExecution = message.failedExecutionFromExecutor(e);
        try {
            logQueue.emitAsync(failedExecution.getLogs());
        } catch (QueueException ex) {
            // fail silently
        }
        return failedExecution.getExecution().getState().isFailed() ? failedExecution.getExecution() : failedExecution.getExecution().withState(State.Type.FAILED);
    }
    
}
