package io.kestra.core.repositories;

import io.kestra.core.lock.Lock;

import java.util.Optional;

public interface LockRepositoryInterface {
    Optional<Lock> findById(String category, String id);

    boolean create(Lock newLock);

    default void delete(Lock existing) {
        deleteById(existing.getCategory(), existing.getId());
    }

    void deleteById(String category, String id);

    int deleteByOwner(String owner);
}
