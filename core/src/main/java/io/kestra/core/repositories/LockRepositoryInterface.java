package io.kestra.core.repositories;

import io.kestra.core.lock.Lock;

import java.util.Optional;

public interface LockRepositoryInterface {
    Optional<Lock> findById(String category, String id);

    boolean create(Lock newLock);

    void delete(Lock existing);

    void deleteById(String category, String id);
}
