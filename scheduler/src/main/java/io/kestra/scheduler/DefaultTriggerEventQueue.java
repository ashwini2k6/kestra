package io.kestra.scheduler;

import io.kestra.core.queues.QueueException;
import io.kestra.core.queues.QueueInterface;
import io.kestra.scheduler.events.TriggerEvent;
import io.kestra.scheduler.internals.Disposable;
import io.kestra.scheduler.internals.VNodes;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;

@Singleton
public class DefaultTriggerEventQueue implements  TriggerEventQueue {
    
    private static final Logger LOG = LoggerFactory.getLogger(DefaultTriggerEventQueue.class);
    
    private final QueueInterface<TriggerEvent> triggerEventQueue;
    
    @Inject
    public DefaultTriggerEventQueue(QueueInterface<TriggerEvent> triggerEventQueue) {
        this.triggerEventQueue = triggerEventQueue;
    }
    
    @Override
    public void send(TriggerEvent event) {
        try {
            triggerEventQueue.emit(event);
        } catch (QueueException e) {
            LOG.error("Failed to send {} event", event.getClass().getSimpleName(), e);
        }
    }
    
    @Override
    public Disposable subscribe(Set<Integer> vNodes, BiConsumer<Integer, TriggerEvent> handler) {
        // TODO - quick and dirty impl
        List<Disposable> disposables = vNodes.stream().map(vNode -> {
            final String consumerGroup = "scheduler-vnode-" + vNode;
            Runnable disposable = triggerEventQueue.receive(consumerGroup, either -> {
                if (either.isLeft()) {
                    TriggerEvent event = either.left().get();
                    int eventVNode = VNodes.computeTriggerVNode(event.id(), 16); //TODO
                    handler.accept(eventVNode, event);
                }
            }, false);
            return Disposable.of(disposable);
        }).toList();
        
        return Disposable.of(disposables);
    }
    
}
