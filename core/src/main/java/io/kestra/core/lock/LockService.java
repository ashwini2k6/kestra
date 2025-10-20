package io.kestra.core.lock;

import io.kestra.core.repositories.LockRepositoryInterface;
import io.kestra.core.server.ServerInstance;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Optional;
import java.util.concurrent.Callable;

/**
 * This service provides facility for executing Runnable and Callable tasks inside a lock.
 * Note: it may be handy to provide a tryLock facility that, if locked, skip executing the Runnable or Callable and exit immediately.
 */
@Slf4j
@Singleton
public class LockService {
    private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(300);
    private static final int DEFAULT_SLEEP_MS = 1;

    private final LockRepositoryInterface lockRepository;

    @Inject
    public LockService(LockRepositoryInterface lockRepository) {
        this.lockRepository = lockRepository;
    }

    /**
     * Executes a Runnable inside a lock.
     * If the lock is already taken, it will wait for at most the default lock timeout of 5mn.
     * @see #doInLock(String, String, Duration, Duration, Runnable)
     *
     * @param category lock category, ex 'executions'
     * @param id identifier of the lock identity inside the category, ex an execution ID
     * @param expiry how much time the lock should be hold before it is considered expired
     *
     * @throws LockException if the lock cannot be hold before the timeout or the thread is interrupted.
     */
    public void doInLock(String category, String id, Duration expiry, Runnable runnable) throws LockException {
        doInLock(category, id, DEFAULT_TIMEOUT, expiry, runnable);
    }

    /**
     * Executes a Runnable inside a lock.
     * If the lock is already taken, it will wait for at most the <code>timeout</code> duration.
     * @see #doInLock(String, String, Duration, Runnable)
     *
     * @param category lock category, ex 'executions'
     * @param id identifier of the lock identity inside the category, ex an execution ID
     * @param timeout how much time to wait for the lock if another process already hold the same lock
     * @param expiry how much time the lock should be hold before it is considered expired
     *
     * @throws LockException if the lock cannot be hold before the timeout or the thread is interrupted.
     */
    public void doInLock(String category, String id, Duration timeout, Duration expiry, Runnable runnable) throws LockException {
        if (!lock(category, id, timeout, expiry)) {
            throw new LockException("Unable to hold the lock inside the configured timeout of " + timeout);
        }

        try {
            runnable.run();
        } finally {
            unlock(category, id);
        }
    }

    /**
     * Executes a Callable inside a lock.
     * If the lock is already taken, it will wait for at most the default lock timeout of 5mn.
     *
     * @param category lock category, ex 'executions'
     * @param id identifier of the lock identity inside the category, ex an execution ID
     * @param expiry how much time the lock should be hold before it is considered expired
     *
     * @throws LockException if the lock cannot be hold before the timeout or the thread is interrupted.
     */
    public <T> T callInLock(String category, String id, Duration expiry, Callable<T> callable) throws Exception {
        return callInLock(category, id, DEFAULT_TIMEOUT, expiry, callable);
    }

    /**
     * Executes a Callable inside a lock.
     * If the lock is already taken, it will wait for at most the <code>timeout</code> duration.
     *
     * @param category lock category, ex 'executions'
     * @param id identifier of the lock identity inside the category, ex an execution ID
     * @param timeout how much time to wait for the lock if another process already hold the same lock
     * @param expiry how much time the lock should be hold before it is considered expired
     *
     * @throws LockException if the lock cannot be hold before the timeout or the thread is interrupted.
     */
    public <T> T callInLock(String category, String id, Duration timeout, Duration expiry, Callable<T> callable) throws Exception {
        if (!lock(category, id, timeout, expiry)) {
            throw new LockException("Unable to hold the lock inside the configured timeout of " + timeout);
        }

        try {
            return callable.call();
        } finally {
            unlock(category, id);
        }
    }

    // TODO should we really expire locks?
    // TODO we need to remove all locks of an instance when it leaves

    private boolean lock(String category, String id, Duration timeout, Duration expiry) throws LockException {
        log.debug("Locking '{}'.'{}'", category,  id);
        // TODO we may want to add a unique generated tx ID
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        do {
            Optional<Lock> existing = lockRepository.findById(category, id);
            if (existing.isEmpty()) {
                // we can try to lock!
                Lock newLock = new Lock(category, id, ServerInstance.INSTANCE_ID, LocalDateTime.now().plus(expiry));
                if (lockRepository.create(newLock)) {
                    return true;
                } else {
                    log.debug("Cannot create the lock, it may have been created after we check for its existence and before we create it");
                }
            } else if (LocalDateTime.now().isAfter(existing.get().getExpiry())) { // check that the lock is not expired
                log.debug("The lock is expired, we take it over");
                // remove the existing lock and try to lock
                lockRepository.delete(existing.get());
                Lock newLock = new Lock(category, id, ServerInstance.INSTANCE_ID, LocalDateTime.now().plus(expiry));
                if (lockRepository.create(newLock)) {
                    return true;
                } else {
                    log.debug("Cannot create the lock, it may have been created after we check for its existence and before we create it");
                }
            } else {
                log.debug("Already locked by: {}", existing.get().getOwner());
            }

            try {
                Thread.sleep(DEFAULT_SLEEP_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new LockException(e);
            }
        } while (System.currentTimeMillis() < deadline);

        log.debug("Lock already hold, waiting for it to be released");
        return false;
    }

    private void unlock(String category, String id) {
        log.debug("Unlocking '{}'.'{}'", category, id);

        Optional<Lock> existing = lockRepository.findById(category, id);
        if (existing.isEmpty()) {
            log.warn("Try to unlock unknown lock '{}'.'{}', ignoring it", category, id);
            return;
        }

        if (!existing.get().getOwner().equals(ServerInstance.INSTANCE_ID)) {
            log.warn("Try to unlock a lock we no longer own '{}'.'{}', ignoring it", category, id);
            return;
        }

        if (existing.get().getExpiry().isAfter(LocalDateTime.now())) {
            // we still need to remove the lock if expired but not taken over as otherwise it would stay locked forever
            log.warn("Unlocking an expired lock '{}'.'{}', this may cause unexpected behaviors", category, id);
        }

        lockRepository.deleteById(category, id);
    }
}
