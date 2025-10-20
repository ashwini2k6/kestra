package io.kestra.core.lock;

import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static io.kestra.core.utils.Rethrow.throwRunnable;
import static org.junit.jupiter.api.Assertions.*;

@KestraTest
@Slf4j
public class LockServiceTest {
    @Inject
    private LockService lockService;

    @Test
    void doInLock() throws LockException {
        lockService.doInLock("category", "doInLock", Duration.ofSeconds(1), () -> log.info("I'm here"));
    }

    @Test
    void doInLockShouldWaitForSameLock() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        Thread.ofVirtual().start(throwRunnable(
            () -> lockService.doInLock("category", "doInLockShouldWaitForSameLock", Duration.ofSeconds(1), throwRunnable(() -> {
                latch.countDown();
                log.info("Start a long transaction");
                Thread.sleep(100);
                log.info("End a long transaction");
            }))
        ));
        // make sure the first transaction begins
        assertTrue(latch.await(1, TimeUnit.SECONDS));

        lockService.doInLock("category", "doInLockShouldWaitForSameLock", Duration.ofSeconds(1), () -> log.info("I'm here"));
    }

    @Test
    void doInLockShouldNotWaitForDifferentLock() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        Thread.ofVirtual().start(throwRunnable(
            () -> lockService.doInLock("category", "doInLockShouldNotWaitForDifferentLock", Duration.ofSeconds(1), throwRunnable(() -> {
                latch.countDown();
                log.info("Start a long transaction");
                Thread.sleep(100);
                log.info("End a long transaction");
            }))
        ));
        // make sure the first transaction begins
        assertTrue(latch.await(1, TimeUnit.SECONDS));

        lockService.doInLock("other", "doInLockShouldNotWaitForDifferentLock", Duration.ofSeconds(1), () -> log.info("I'm here"));
    }

    @Test
    void doInLockShouldFailForSameLock() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        Thread.ofVirtual().start(throwRunnable(
            () -> lockService.doInLock("category", "doInLockShouldFailForSameLock", Duration.ofSeconds(10), throwRunnable(() -> {
                latch.countDown();
                log.info("Start a long transaction");
                Thread.sleep(500);
                log.info("End a long transaction");
            }))
        ));
        // make sure the first transaction begins
        assertTrue(latch.await(1, TimeUnit.SECONDS));

        assertThrows(LockException.class, () ->
            lockService.doInLock("category", "doInLockShouldFailForSameLock", Duration.ofMillis(100), Duration.ofSeconds(10), () -> log.info("I'm here"))
        );
    }

    @Test
    void doInLockShouldNotFailForDifferentLock() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        Thread.ofVirtual().start(throwRunnable(
            () -> lockService.doInLock("category", "doInLockShouldNotFailForDifferentLock", Duration.ofSeconds(10), throwRunnable(() -> {
                latch.countDown();
                log.info("Start a long transaction");
                Thread.sleep(500);
                log.info("End a long transaction");
            }))
        ));
        // make sure the first transaction begins
        assertTrue(latch.await(1, TimeUnit.SECONDS));

        lockService.doInLock("other", "doInLockShouldNotFailForDifferentLock", Duration.ofMillis(100), Duration.ofSeconds(10), () -> log.info("I'm here"));
    }
}