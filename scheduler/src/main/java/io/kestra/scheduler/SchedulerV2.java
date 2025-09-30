package io.kestra.scheduler;

import com.google.common.annotations.VisibleForTesting;
import io.kestra.core.runners.Scheduler;
import io.kestra.core.server.ServiceStateChangeEvent;
import io.kestra.core.server.ServiceType;
import io.kestra.core.services.AbstractService;
import io.kestra.core.utils.ExecutorsUtils;
import io.kestra.scheduler.internals.Disposable;
import io.kestra.scheduler.internals.SchedulerClock;
import io.kestra.scheduler.pubsub.TriggerWorkerJobResultSubscriber;
import io.micronaut.context.annotation.Primary;
import io.micronaut.context.event.ApplicationEventPublisher;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

@Slf4j
@Singleton
@Primary
public class SchedulerV2 extends AbstractService implements Scheduler {
    
    private static final String EXECUTOR_NAME = "scheduler";
    
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final ExecutorsUtils executorsUtils;
    private final TriggerSchedulingLoopFactory schedulerEventLoopFactory;
    
    private ExecutorService executorService;
    private List<TriggerSchedulingLoop> schedulingLoops;
    private final DefaultTriggerAssigner triggerAssigner;
    private final Clock clock;
    
    // Queues
    private final TriggerEventQueue triggerEventQueue;
    
    // Services
    private final TriggerWorkerJobResultSubscriber triggerWorkerJobResultSubscriber;
    
    // Consumers
    private final List<Disposable> consumerDisposables = new ArrayList<>();
    
    @Inject
    public SchedulerV2(final TriggerSchedulingLoopFactory schedulerEventLoopFactory,
                       final DefaultTriggerAssigner triggerAssigner,
                       final ExecutorsUtils executorsUtils,
                       final ApplicationEventPublisher<ServiceStateChangeEvent> eventPublisher,
                       final TriggerEventQueue triggerEventQueue,
                       final TriggerWorkerJobResultSubscriber triggerWorkerJobResultSubscriber) {
        this(schedulerEventLoopFactory, triggerAssigner, executorsUtils, eventPublisher, triggerEventQueue, triggerWorkerJobResultSubscriber, SchedulerClock.getClock());
    }
    
    @VisibleForTesting
    public SchedulerV2(final TriggerSchedulingLoopFactory schedulerEventLoopFactory,
                       final DefaultTriggerAssigner triggerAssigner,
                       final ExecutorsUtils executorsUtils,
                       final ApplicationEventPublisher<ServiceStateChangeEvent> eventPublisher,
                       final TriggerEventQueue triggerEventQueue,
                       final TriggerWorkerJobResultSubscriber triggerWorkerJobResultSubscriber,
                       final Clock clock) {
        super(ServiceType.SCHEDULER, eventPublisher);
        this.schedulerEventLoopFactory = schedulerEventLoopFactory;
        this.executorsUtils = executorsUtils;
        this.triggerAssigner = triggerAssigner;
        this.triggerEventQueue = triggerEventQueue;
        this.triggerWorkerJobResultSubscriber = triggerWorkerJobResultSubscriber;
        this.clock = clock;
        this.setState(ServiceState.CREATED);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void run() {
        start(Math.max(4, Runtime.getRuntime().availableProcessors()));
    }
    
    public void start(int numThreads) {
        if (!this.started.compareAndSet(false, true)) {
            throw new IllegalStateException("Scheduler already started");
        }
        
        // Create the scheduling loops
        this.executorService = executorsUtils.maxCachedThreadPool(numThreads, EXECUTOR_NAME);
        this.schedulingLoops = new ArrayList<>(numThreads);
        for (int i = 0; i < numThreads; i++) {
            TriggerSchedulingLoop schedulingLoop = this.schedulerEventLoopFactory.create(i, this.clock);
            this.schedulingLoops.add(schedulingLoop);
            this.executorService.submit(schedulingLoop);
        }
        
        // Subscribe to trigger vNodes assignment/revocation
        triggerAssigner.subscribe(this.getId(), new DefaultTriggerAssigner.TriggerAssignmentListener() {
            @Override
            public void onTriggerAssignmentRevoked() {
                
                // Stop the WorkerTriggerResult/TriggerEvent Queues consumption
                stopAllConsumers();
                
                // Stop all scheduling loops
                List<CompletableFuture<Void>> pausable = schedulingLoops.stream().map(schedulingLoop ->
                    schedulingLoop.doOnEndLoop(() -> {
                        // Pause the scheduling loop
                        schedulingLoop.pause();
                        
                        // Ensure all the trigger events for this scheduling loop are processed
                        schedulingLoop.processTriggerEvents();
                        
                        // Revoke all vNodes assignment
                        schedulingLoop.setAssignments(Set.of());
                    })).toList();
                
                // Wait for all scheduling loop to be effectively paused
                CompletableFuture.allOf(pausable.toArray(new CompletableFuture[0])).join();
            }
            
            @Override
            public void onTriggerAssignmentAssigned(Set<Integer> vNodes) {
                
                int numSchedulingLoop = schedulingLoops.size();
                schedulingLoops.forEach(schedulingLoop -> {
                    // Compute vNodes assignments for the current event-loop
                    Set<Integer> assignments = vNodes.stream()
                        .filter(vNodeId -> vNodeId % numSchedulingLoop == schedulingLoop.id())
                        .collect(Collectors.toSet());
                    
                    // Set vNodes assignment
                    schedulingLoop.setAssignments(assignments);
                    
                    // Resume the scheduling loop
                    schedulingLoop.resume();
                });
                
                // Restart the TriggerEvent Queue consumption
                startTriggerEventConsumers(vNodes);
                startTriggerResultConsumer();
            }
        });
        
        log.info("Scheduler started with {} thread(s)", numThreads);
        setState(ServiceState.RUNNING);
    }
    
    
    private void stopAllConsumers() {
        consumerDisposables.forEach(Disposable::dispose);
    }
    
    private void startTriggerResultConsumer() {
        consumerDisposables.add(triggerWorkerJobResultSubscriber.subscribe(clock));
    }
    
    private void startTriggerEventConsumers(final Set<Integer> vNodes) {
        Map<Integer, TriggerSchedulingLoop> schedulingLoopByVNode = getSchedulingLoopByVNode();
        consumerDisposables.add(triggerEventQueue.subscribe(vNodes, (vNode, event) -> {
            TriggerSchedulingLoop schedulingLoop = schedulingLoopByVNode.get(vNode);
            if (schedulingLoop != null) {
                schedulingLoop.addTriggerEvent(vNode, event);
            }
        }));
    }
    
    private Map<Integer, TriggerSchedulingLoop> getSchedulingLoopByVNode() {
        return schedulingLoops.stream()
            .flatMap(schedulingLoop ->
                schedulingLoop.assignments().stream().map(vNodeAssignment -> Map.entry(vNodeAssignment, schedulingLoop))
            )
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected ServiceState doStop() {
        if (!this.started.compareAndSet(true, false)) {
            return ServiceState.TERMINATED_GRACEFULLY; // Already shut down or not started.
        }
        // Notify all TriggerSchedulingLoop to stop
        this.schedulingLoops.forEach(TriggerSchedulingLoop::stop);
        
        // Initiate graceful shutdown
        this.executorService.shutdown();
        
        // Wait for all TriggerSchedulingLoop to terminate
        boolean terminated;
        try {
            terminated = this.executorService.awaitTermination(1, TimeUnit.MINUTES);
            if (terminated) {
                log.warn("Forcing scheduler shutdown...");
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            executorService.shutdownNow();
            terminated = false;
            log.warn("Interrupted while stopping scheduler. Forced shutdown initiated.");
        }
        
        if (!terminated) {
            log.warn("Scheduler still has pending loops after shutdown. Forced termination completed.");
        }
        return terminated ? ServiceState.TERMINATED_GRACEFULLY : ServiceState.TERMINATED_FORCED;
    }
}
