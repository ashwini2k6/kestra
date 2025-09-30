package io.kestra.scheduler;

import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.scheduler.events.ResetTrigger;
import io.kestra.scheduler.events.TriggerCreated;
import io.kestra.scheduler.events.TriggerDeleted;
import io.kestra.scheduler.events.TriggerEvent;
import io.kestra.scheduler.events.TriggerEventHandler;
import io.kestra.scheduler.events.TriggerExecuted;
import io.kestra.scheduler.events.TriggerUpdated;
import io.kestra.scheduler.internals.NextEvaluationDate;
import io.kestra.scheduler.models.TriggerState;
import io.kestra.scheduler.stores.TriggerStateStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class TriggerSchedulingLoop implements Runnable {
    
    private static final Logger LOG = LoggerFactory.getLogger(TriggerSchedulingLoop.class);
    
    private static final long SCHEDULE_INTERVAL_MILLIS = Duration.ofSeconds(1).toMillis();
    
    private final int schedulerEventLoopId;
    private final TriggerScheduler triggerScheduler;
    private final Clock clock;
    
    private final BlockingQueue<TriggerEventAndVNode> triggerEventQueue = new LinkedBlockingQueue<>();
    
    // Services
    private TriggerEventHandler triggerEventHandler;
    
    // Threading
    private volatile Thread thread;
    private volatile boolean initialized = false;
    
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final CountDownLatch stopped = new CountDownLatch(1);
    private final AtomicBoolean paused = new AtomicBoolean(false);
    private final ReentrantLock pauseLock = new ReentrantLock();
    private final Condition unpaused = pauseLock.newCondition();
    
    private final BlockingQueue<Runnable> internalLoopCallables = new LinkedBlockingQueue<>();
    
    private final Set<Integer> assignments = new HashSet<>();
    
    public TriggerSchedulingLoop(int schedulerEventLoopId,
                                 TriggerScheduler triggerScheduler,
                                 TriggerEventHandler triggerEventHandler,
                                 Clock clock) {
        this.schedulerEventLoopId = schedulerEventLoopId;
        this.triggerScheduler = triggerScheduler;
        this.triggerEventHandler = triggerEventHandler;
        this.clock = clock;
    }
    
    /**
     * 
     * @return  the ID of this event-loop (
     */
    public int id() {
        return this.schedulerEventLoopId;
    }
    
    /** {@inheritDoc} **/
    @Override
    public void run() {
        if (!this.running.compareAndSet(false, true)) {
            throw new IllegalStateException("Already running");
        }
        
        this.thread = Thread.currentThread();
        Instant lastScheduleTime = null;
        try {
            while (running.get()) {
                try {
                    waitIfPaused();
                    
                    // Check whether vNodes are available for this event-loop
                    // The list of vNodes assignments can be empty if:
                    // 1. This scheduler is starting
                    // 2. The vNode assignments was cleared
                    if (assignments.isEmpty()) {
                        doOnEndLoop();
                        if (assignments.isEmpty()) {
                            waitBeforeNextLoop(Duration.ofMillis(50));
                        }
                        continue;
                    }
                    
                    final Instant now = clock.instant();
                    
                    if (!initialized) {
                        triggerScheduler.onStart(clock, now, assignments);
                        initialized = true;
                    }
                    
                    // Process all received triggers events for current assignments.
                    processTriggerEvents();
                    
                    // Check whether triggers should be scheduled
                    boolean shouldScheduleTriggers = lastScheduleTime == null || now.toEpochMilli() - lastScheduleTime.toEpochMilli() >= SCHEDULE_INTERVAL_MILLIS;
                    
                    if (shouldScheduleTriggers) {
                        triggerScheduler.onSchedule(clock, now, assignments);
                        lastScheduleTime = now;
                    }
                    
                    doOnEndLoop();
                    
                    // If no events arrived in meanwhile wait before next loop
                    if (triggerEventQueue.isEmpty()) {
                        long elapsed = now.toEpochMilli() - lastScheduleTime.toEpochMilli();
                        long remaining = SCHEDULE_INTERVAL_MILLIS - elapsed;
                        if (remaining > 0) {
                            waitBeforeNextLoop(Duration.ofMillis(remaining));
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    LOG.warn("Interrupted while waiting in scheduling loop. Stopping.");
                    running.set(false);
                } catch (Exception e) {
                    LOG.error("Error in scheduling loop", e);
                }
            }
        } finally {
            stopped.countDown();
            LOG.info("[{}-{}] stopped", getClass().getSimpleName(), schedulerEventLoopId);
        }
    }
    
    private void waitBeforeNextLoop(final Duration duration) throws InterruptedException {
        Thread.sleep(duration);
    }
    
    public synchronized void stop() {
        if (!running.compareAndSet(true, false)) {
            LOG.debug("[{}] stop() called but not running", getClass().getSimpleName());
            return;
        }
        
        resume(); // In case it's paused and blocked
        
        if (this.thread != null) {
            this.thread.interrupt();
            try {
                if (!stopped.await(5, TimeUnit.SECONDS)) {
                    LOG.warn("Timeout while waiting for {} to complete", this.thread.getName());
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
    
    private void waitIfPaused() throws InterruptedException {
        pauseLock.lock();
        try {
            while (paused.get() && running.get()) {
                LOG.info("Paused. Waiting for scheduling loop to resume");
                unpaused.await(); // Wait until resume() signals
                LOG.info("Resumed");
            }
        } finally {
            pauseLock.unlock();
        }
    }
    
    public Set<Integer> assignments() {
        return assignments;
    }
    
    public void setAssignments(final Set<Integer> assignments) {
        this.assignments.clear();
        if (assignments != null) {
            this.assignments.addAll(assignments);
        }
        this.initialized = false;
    }
    
    /**
     * Pauses this event-loop instance.
     */
    public void pause() {
        paused.set(true);
    }
    
    public CompletableFuture<Void> doOnEndLoop(final Runnable action) {
        final CompletableFuture<Void> future = new CompletableFuture<>();
        internalLoopCallables.add(() -> {
            try {
                action.run();
                future.complete(null);
            } catch (Exception e) {
                future.completeExceptionally(e);
            }
        });
        return future;
    }
    
    /**
     * Resumes this event-loop instance if currently paused.
     */
    public void resume() {
        pauseLock.lock();
        try {
            if (paused.compareAndSet(true, false)) {
                unpaused.signalAll();
            }
        } finally {
            pauseLock.unlock();
        }
    }
    
    private void doOnEndLoop() {
        List<Runnable> drained = new ArrayList<>();
        internalLoopCallables.drainTo(drained);

        for (Runnable runnable : drained) {
            runnable.run();
        }
    }
    
    /**
     * 
     * @param event The trigger event.
     */
    public void addTriggerEvent(int vNode, TriggerEvent event) {
        this.triggerEventQueue.add(new TriggerEventAndVNode(event, vNode));
    }
    
    /**
     * Processes all trigger events currently queued by this scheduling loop.
     * 
     * @return the number of events processed.
     */
    public int processTriggerEvents() {
        List<TriggerEventAndVNode> drained = new ArrayList<>();
        triggerEventQueue.drainTo(drained);
        drained.forEach(item -> triggerEventHandler.handle(clock, item.vnode(), item.event()));
        return drained.size();
    }
    
    private record TriggerEventAndVNode(TriggerEvent event, Integer vnode) {
        
    }
}
