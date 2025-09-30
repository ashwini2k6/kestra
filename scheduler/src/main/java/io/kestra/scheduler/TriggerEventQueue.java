package io.kestra.scheduler;

import io.kestra.scheduler.events.TriggerEvent;
import io.kestra.scheduler.internals.Disposable;

import java.util.Set;
import java.util.function.BiConsumer;

public interface TriggerEventQueue {
    
    void send(TriggerEvent triggerEvent);
    
    Disposable subscribe(Set<Integer> vNodes, BiConsumer<Integer, TriggerEvent> handler);
}
