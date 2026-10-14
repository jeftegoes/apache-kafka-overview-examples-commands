package com.example.consumer.exceptions;

public class RetryableException extends RuntimeException {
    public RetryableException(Throwable cause) {
        super(cause);
    }

    public RetryableException(String cause) {
        super(cause);
    }
}
