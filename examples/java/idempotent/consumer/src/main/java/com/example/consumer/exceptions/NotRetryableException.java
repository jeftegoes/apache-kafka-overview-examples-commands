package com.example.consumer.exceptions;

public class NotRetryableException extends RuntimeException {
    public NotRetryableException(Throwable message) {
        super(message);
    }

    public NotRetryableException(String message) {
        super(message);
    }
}
