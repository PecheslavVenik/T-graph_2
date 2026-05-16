package com.pm.graph_api_v2.exception;

public class ApiBadRequestException extends RuntimeException {

    private final Object details;

    public ApiBadRequestException(String message) {
        this(message, null);
    }

    public ApiBadRequestException(String message, Object details) {
        super(message);
        this.details = details;
    }

    public Object getDetails() {
        return details;
    }
}
