package io.kestra.core.lock;

public class LockException extends Exception {
    public LockException(String message) {
        super(message);
    }

    public LockException(InterruptedException e) {
        super(e);
    }
}
