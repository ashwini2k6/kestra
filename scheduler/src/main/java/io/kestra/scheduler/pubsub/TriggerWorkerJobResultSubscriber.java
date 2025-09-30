package io.kestra.scheduler.pubsub;

import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.triggers.PollingTriggerInterface;
import io.kestra.core.models.triggers.RealtimeTriggerInterface;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.queues.QueueInterface;
import io.kestra.core.runners.Scheduler;
import io.kestra.core.runners.WorkerTriggerResult;
import io.kestra.scheduler.TriggerEventQueue;
import io.kestra.scheduler.events.TriggerExecuted;
import io.kestra.scheduler.internals.Disposable;
import io.kestra.scheduler.internals.NextEvaluationDate;
import io.kestra.scheduler.models.TriggerId;
import io.kestra.scheduler.models.TriggerState;
import io.kestra.scheduler.stores.TriggerStateStore;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;

@Singleton
public class TriggerWorkerJobResultSubscriber {
    
    private static final Logger LOG = LoggerFactory.getLogger(TriggerWorkerJobResultSubscriber.class);
    
    // Services
    private final TriggerExecutionPublisher triggerExecutionSender;
    
    // Queues
    private final QueueInterface<WorkerTriggerResult> workerTriggerResultQueue;
    private final TriggerEventQueue triggerEventQueue;
    
    
    /**
     * Creates a new {@link TriggerWorkerJobResultSubscriber} instance.
     *
     * @param workerTriggerResultQueue The WorkerTriggerResult queue.
     */
    public TriggerWorkerJobResultSubscriber(final QueueInterface<WorkerTriggerResult> workerTriggerResultQueue,
                                            final TriggerEventQueue triggerEventQueue,
                                            final TriggerExecutionPublisher triggerExecutionSender) {
        this.workerTriggerResultQueue = workerTriggerResultQueue;
        this.triggerExecutionSender = triggerExecutionSender;
        this.triggerEventQueue = triggerEventQueue;
    }
    
    public Disposable subscribe(final Clock clock) {
        return Disposable.of(this.workerTriggerResultQueue.receive(
            null,
            Scheduler.class,
            either -> {
                if (either.isRight()) {
                    LOG.error("Unable to deserialize a worker trigger result: {}", either.getRight().getMessage());
                    return;
                }
                
                WorkerTriggerResult workerTriggerResult = either.getLeft();
                TriggerContext triggerContext = workerTriggerResult.getTriggerContext();
                
                // Get if an Execution is attached to the TriggerResult.
                Execution execution = workerTriggerResult.getExecution().orElse(null);
                
                if (workerTriggerResult.getTrigger() instanceof PollingTriggerInterface) {
                    triggerEventQueue.send(new TriggerExecuted(TriggerId.of(triggerContext), clock.instant(), execution));
                }
                else if (workerTriggerResult.getTrigger() instanceof RealtimeTriggerInterface && execution != null) {
                    triggerExecutionSender.sendExecution(workerTriggerResult.getExecution().get(), triggerContext);
                }
            }
        ));
    }
}
