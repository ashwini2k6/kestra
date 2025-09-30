package io.kestra.scheduler.stores;

import io.kestra.core.models.flows.Flow;
import io.kestra.core.models.flows.FlowInterface;
import io.kestra.core.models.flows.FlowWithSource;
import io.kestra.core.repositories.FlowRepositoryInterface;
import io.kestra.scheduler.SchedulerConfig;
import io.kestra.scheduler.internals.VNodes;
import jakarta.inject.Inject;

import java.util.List;
import java.util.Optional;
import java.util.Set;

public class FlowStateStore {
    
    private final SchedulerConfig schedulerConfig;
    private final FlowRepositoryInterface flowRepository;
    
    @Inject
    public FlowStateStore(final SchedulerConfig schedulerConfig, final FlowRepositoryInterface flowRepository) {
        this.flowRepository = flowRepository;
        this.schedulerConfig = schedulerConfig;
    }
    
    public Optional<FlowWithSource> findFlow(String tenant, String namespace, String flowId) {
        return this.flowRepository.findByIdWithSource(tenant, namespace, flowId);
    }
    
    public List<Flow> findAllFlowsForVNodes(final Set<Integer> vNodes) {
        return this.flowRepository.findAllForAllTenants()
            .stream()
            .filter(f -> vNodes.contains(VNodes.computeFlowVNode(f, schedulerConfig.vnodes())))
            .toList();
    }
}
