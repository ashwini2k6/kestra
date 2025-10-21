package io.kestra.core.repositories;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.lock.Lock;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

@KestraTest
public abstract class AbstractLockRepositoryTest {
    @Inject
    private LockRepositoryInterface lockRepository;

    @Test
    void findById() {
        var lock = Lock.builder().category("test").id("findById").owner("me").build();
        boolean created = lockRepository.create(lock);
        assertThat(created).isTrue();

        var existing = lockRepository.findById("test", "findById");
        assertThat(existing).isPresent();
        assertThat(existing.get().getCategory()).isEqualTo("test");
        assertThat(existing.get().getOwner()).isEqualTo("me");
        assertThat(existing.get().getId()).isEqualTo("findById");

        lockRepository.delete(lock);
    }

    @Test
    void create() {
        var lock = Lock.builder().category("test").id("create").owner("me").build();
        boolean created = lockRepository.create(lock);
        assertThat(created).isTrue();

        boolean ignored = lockRepository.create(lock);
        assertThat(ignored).isFalse();

        lockRepository.delete(lock);
    }

    @Test
    void delete() {
        var lock = Lock.builder().category("test").id("delete").owner("me").build();
        boolean created = lockRepository.create(lock);
        assertThat(created).isTrue();

        lockRepository.delete(lock);

        var existing = lockRepository.findById("test", "delete");
        assertThat(existing).isEmpty();
    }

    @Test
    void deleteById() {
        var lock = Lock.builder().category("test").id("deleteById").owner("me").build();
        boolean created = lockRepository.create(lock);
        assertThat(created).isTrue();

        lockRepository.deleteById("test", "deleteById");

        var existing = lockRepository.findById("test", "deleteById");
        assertThat(existing).isEmpty();
    }

    @Test
    void deleteByOwner() {
        var lock = Lock.builder().category("test").id("deleteByOwner").owner("me").build();
        boolean created = lockRepository.create(lock);
        assertThat(created).isTrue();

        int deleted = lockRepository.deleteByOwner("me");
        assertThat(deleted).isEqualTo(1);

        var existing = lockRepository.findById("test", "deleteByOwner");
        assertThat(existing).isEmpty();
    }
}
