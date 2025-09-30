package io.kestra.scheduler.stores;

import com.google.common.annotations.VisibleForTesting;
import io.kestra.core.models.flows.Flow;
import io.kestra.core.models.triggers.Trigger;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.repositories.TriggerRepositoryInterface;
import io.kestra.scheduler.SchedulerConfig;
import io.kestra.scheduler.internals.VNodes;
import io.kestra.scheduler.models.TriggerId;
import io.kestra.scheduler.models.TriggerState;
import io.kestra.scheduler.models.TriggerStatus;
import jakarta.inject.Inject;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class TriggerStateStore {
    
    private final TriggerRepositoryInterface triggerRepository;
    private final SchedulerConfig schedulerConfig;
    
    @Inject
    public TriggerStateStore(SchedulerConfig schedulerConfig, 
                             TriggerRepositoryInterface triggerRepository) {
        this.triggerRepository = triggerRepository;
        this.schedulerConfig = schedulerConfig;
    }
    
    public List<TriggerState> findByNextExecutionDateReadyForAllTenants(ZonedDateTime now, Set<Integer> vNodes) {
        return triggerRepository.findByNextExecutionDateReadyForAllTenants(now, vNodes)
            .stream()
            .map(TriggerStateAdapter::fromTrigger)
            .toList();
    }
    
    public List<TriggerState> findForVNodes(final Set<Integer> vNodes) {
        return this.triggerRepository.findAllForAllTenants()
            .stream()
            .filter(f -> vNodes.contains(VNodes.computeTriggerVNode(TriggerId.of(f), schedulerConfig.vnodes())))
            .map(TriggerStateAdapter::fromTrigger)
            .toList();
    }
    
    public Optional<TriggerState> find(TriggerId triggerId) {
        return doFind(triggerId).map(TriggerStateAdapter::fromTrigger);
    }
    
    public void save(TriggerState triggerState) {
        Trigger entity = TriggerStateAdapter.toTrigger(triggerState);
        triggerRepository.save(entity);
    }
    
    public void delete(TriggerId triggerId) {
        doFind(triggerId).ifPresent(triggerRepository::delete);
    }
    
    private Optional<Trigger> doFind(TriggerId triggerId) {
        return triggerRepository.findLast(TriggerContext.builder()
            .tenantId(triggerId.tenant())
            .namespace(triggerId.namespace())
            .flowId(triggerId.flowId())
            .triggerId(triggerId.triggerId())
            .build()
        );
    }
    
    @VisibleForTesting
    static class TriggerStateAdapter{
        
        public static TriggerState fromTrigger(Trigger trigger) {
            return new TriggerState(
                trigger,
                trigger.getUpdatedDate(), 
                null, 
                TriggerStatus.IDLE,
                trigger.getExecutionId(), 
                0L // TODO
            );
        }
        
        public static Trigger toTrigger(TriggerState triggerState) {
            return Trigger.builder()
                .tenantId(triggerState.context().getTenantId())
                .namespace(triggerState.context().getNamespace())
                .flowId(triggerState.context().getFlowId())
                .triggerId(triggerState.context().getTriggerId())
                .date(triggerState.context().getDate())
                .backfill(triggerState.context().getBackfill())
                .stopAfter(triggerState.context().getStopAfter())
                .disabled(triggerState.context().getDisabled())
                .nextExecutionDate(triggerState.context().getNextExecutionDate())
                .executionId(triggerState.executionId())
                .vnode(triggerState.context().getVnode())
                .build();
        }
        
    }
}
