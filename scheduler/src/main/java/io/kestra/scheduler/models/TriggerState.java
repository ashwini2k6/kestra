package io.kestra.scheduler.models;

import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.flows.State;
import io.kestra.core.models.triggers.Backfill;
import io.kestra.core.models.triggers.TriggerContext;

import java.time.Clock;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Objects;

/**
 * Represents the state of a trigger.
 *
 * @param context
 * @param updatedAt     the timestamp when this trigger state was last updated.
 * @param evaluatedAt   the timestamp when the trigger was last evaluated.
 * @param executionId   the identifier of the currently running execution produced by this trigger, or {@code null} if no execution is active.
 * @param seqNo         the sequence number of this state; used for optimistic concurrency control
 */
public record TriggerState(
    TriggerContext context,
    Instant updatedAt,
    Instant evaluatedAt,
    TriggerStatus status,
    String executionId,
    Long seqNo
) {
    
    /**
     * Factory method for constructing a new {@link TriggerState} from a given {@link TriggerContext}.
     *
     * @param context of the trigger.
     * @return a new {@link TriggerState}
     */
    public static TriggerState of(TriggerContext context) {
        return new TriggerState(context, null, null, TriggerStatus.IDLE, null,0L);
    }
    
    /**
     * Updates the status of this trigger state.
     *
     * @param clock   the scheduler clock.
     * @return a new {@link TriggerState}
     */
    public TriggerState status(final Clock clock, final TriggerStatus status) {
        return new TriggerState(
            context,
            clock.instant(),
            evaluatedAt,
            status,
            executionId,
            seqNo
        );
    }
    
    /**
     * Updates the evaluatedAt of this trigger state.
     *
     * @param clock   the scheduler clock.
     * @return a new {@link TriggerState}
     */
    public TriggerState evaluatedAt(final Clock clock, final Instant evaluatedAt) {
        return new TriggerState(
            context,
            clock.instant(),
            evaluatedAt,
            status,
            executionId,
            seqNo
        );
    }
    
    /**
     * Updates the state of the trigger for the given  {@code nextEvaluationDate}.
     *
     * @param clock              the scheduler clock.
     * @param nextEvaluationDate the next evaluation date.
     * @return a new {@link TriggerState}
     */
    public TriggerState updateForNextEvaluationDate(final Clock clock, final ZonedDateTime nextEvaluationDate) {
        return new TriggerState(
            updateContextWithNextExecutionDate(context, nextEvaluationDate),
            clock.instant(),
            evaluatedAt,
            status,
            executionId,
            seqNo
        );
    }
    
    /**
     * Updates the state of the trigger for the given {@link Execution}.
     *
     * @param clock              the scheduler clock.
     * @param execution          the execution.
     * @return a new {@link TriggerState}
     */
    public TriggerState updateForExecution(final Clock clock, final Execution execution) {
        return new TriggerState(
            context,
            clock.instant(),
            evaluatedAt,
            status,
            execution.getId(),
            seqNo
        ).updateForExecutionState(clock, execution.getState().getCurrent());
    }
    
    /**
     * Updates the state of the trigger for the given execution state.
     *
     * @param clock the scheduler clock.
     * @param state the execution state.
     * @return a new {@link TriggerState}
     */
    public TriggerState updateForExecutionState(final Clock clock, final State.Type state) {
        // switch disabled automatically if the executionEndState is one of the stopAfter states
        Boolean disabled = context.getStopAfter() != null ? context.getStopAfter().contains(state) : context.getDisabled();
        
        if (Objects.equals(context.getDisabled(), disabled)) {
            return this;
        }
        
        return new TriggerState(
            context.toBuilder().disabled(disabled).build(),
            clock.instant(),
            evaluatedAt,
            status,
            State.Type.FAILED.equals(state) ? null : executionId,
            seqNo
        );
    }
    
    private static TriggerContext updateContextWithNextExecutionDate(final TriggerContext context, final ZonedDateTime nextExecutionDate) {
        
        TriggerContext.TriggerContextBuilder<?, ?> builder = context.toBuilder()
            .nextExecutionDate(nextExecutionDate);
        
        if (context.getBackfill() != null && !context.getBackfill().getPaused()) {
            Backfill backfill = context.getBackfill();
            if (nextExecutionDate.isAfter(backfill.getEnd())) {
                return builder
                    .nextExecutionDate(backfill.getPreviousNextExecutionDate())
                    .backfill(null)
                    .build();
            } else {
                return builder
                    .backfill(backfill.toBuilder().currentDate(nextExecutionDate).build())
                    .build();
            }
        }
        return builder.build();
    }
    
}
