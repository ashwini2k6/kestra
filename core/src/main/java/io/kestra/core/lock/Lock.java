package io.kestra.core.lock;

import io.kestra.core.utils.IdUtils;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Lock {
    private String category;
    private String id;
    private String owner;
    private LocalDateTime expiry;

    public String uid() {
        return IdUtils.fromParts(this.category, this.id);
    }
}
