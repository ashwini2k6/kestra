package io.kestra.scheduler.models;

import io.kestra.core.models.HasUID;
import io.kestra.core.models.flows.FlowWithSource;
import io.kestra.core.models.triggers.AbstractTrigger;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.utils.IdUtils;

/**
 * Represents a fully qualified trigger identifier.
 * 
 * @param tenant
 * @param namespace
 * @param flowId
 * @param triggerId
 */
public record TriggerId(
    String tenant,
    String namespace,
    String flowId,
    String triggerId
) implements HasUID {
    
    public static TriggerId of(FlowWithSource flow, AbstractTrigger trigger) {
        return new TriggerId(flow.getTenantId(), flow.getNamespace(), flow.getId(), trigger.getId());
    }
    
    public static TriggerId of(TriggerContext context) {
        return new TriggerId(
            context.getTenantId(),
            context.getNamespace(),
            context.getFlowId(),
            context.getTriggerId()
        );
    }
    
    @Override
    public String uid() {
        return IdUtils.fromParts(
            tenant,
            namespace,
            flowId,
            triggerId
        );
    }
}
