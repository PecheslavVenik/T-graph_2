package com.pm.graph_api_v2.exception;

import com.pm.graph_api_v2.config.TraceIdFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

import java.util.List;
import java.util.Map;

@RestControllerAdvice
public class ApiExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(ApiExceptionHandler.class);

    @ExceptionHandler(ApiBadRequestException.class)
    public ProblemDetail handleBadRequest(ApiBadRequestException ex) {
        log.warn("Bad request traceId={} message={}", traceId(), ex.getMessage());
        return problem(HttpStatus.BAD_REQUEST, "BAD_REQUEST", ex.getMessage(), null);
    }

    @ExceptionHandler(ApiNotFoundException.class)
    public ProblemDetail handleNotFound(ApiNotFoundException ex) {
        log.info("Not found traceId={} message={}", traceId(), ex.getMessage());
        return problem(HttpStatus.NOT_FOUND, "NOT_FOUND", ex.getMessage(), null);
    }

    @ExceptionHandler(ApiServiceUnavailableException.class)
    public ProblemDetail handleServiceUnavailable(ApiServiceUnavailableException ex) {
        log.warn("Service unavailable traceId={} message={}", traceId(), ex.getMessage());
        return problem(HttpStatus.SERVICE_UNAVAILABLE, "SERVICE_UNAVAILABLE", ex.getMessage(), null);
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ProblemDetail handleValidation(MethodArgumentNotValidException ex) {
        List<Map<String, String>> fields = ex.getBindingResult()
            .getFieldErrors()
            .stream()
            .map(this::toMap)
            .toList();

        log.warn("Validation failed traceId={} fieldCount={}", traceId(), fields.size());
        return problem(HttpStatus.BAD_REQUEST, "VALIDATION_ERROR", "Validation failed", fields);
    }

    @ExceptionHandler(MethodArgumentTypeMismatchException.class)
    public ProblemDetail handleTypeMismatch(MethodArgumentTypeMismatchException ex) {
        String message = "Invalid value for parameter: " + ex.getName();
        log.warn("Type mismatch traceId={} parameter={} value={}", traceId(), ex.getName(), ex.getValue());
        return problem(HttpStatus.BAD_REQUEST, "BAD_REQUEST", message, null);
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ProblemDetail handleUnreadableBody(HttpMessageNotReadableException ex) {
        log.warn("Malformed JSON traceId={} message={}", traceId(), ex.getMessage());
        return problem(HttpStatus.BAD_REQUEST, "BAD_REQUEST", "Malformed request body", null);
    }

    @ExceptionHandler(Exception.class)
    public ProblemDetail handleGeneric(Exception ex) {
        log.error("Unexpected server error traceId={}", traceId(), ex);
        return problem(HttpStatus.INTERNAL_SERVER_ERROR, "INTERNAL_ERROR", "Unexpected server error", null);
    }

    private ProblemDetail problem(HttpStatus status, String code, String message, Object details) {
        ProblemDetail pd = ProblemDetail.forStatus(status);
        pd.setTitle(code);
        pd.setDetail(message);
        pd.setProperty("code", code);
        pd.setProperty("message", message);
        pd.setProperty("traceId", traceId());
        if (details != null) {
            pd.setProperty("details", details);
        }
        return pd;
    }

    private Map<String, String> toMap(FieldError error) {
        return Map.of(
            "field", error.getField(),
            "message", error.getDefaultMessage() == null ? "invalid" : error.getDefaultMessage()
        );
    }

    private String traceId() {
        return MDC.get(TraceIdFilter.TRACE_ID);
    }
}
