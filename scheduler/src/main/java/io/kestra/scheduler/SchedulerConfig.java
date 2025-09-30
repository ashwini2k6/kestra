package io.kestra.scheduler;

import io.micronaut.context.annotation.ConfigurationProperties;
import io.micronaut.core.bind.annotation.Bindable;

@ConfigurationProperties("kestra.scheduler")
public record SchedulerConfig(
    @Bindable(defaultValue = "16")
    Integer vnodes
) {
}
