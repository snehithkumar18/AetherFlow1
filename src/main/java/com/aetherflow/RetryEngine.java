package com.aetherflow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;





public class RetryEngine {
    
    
    public static class RetryPolicy {
        public final int maxRetries;
        public final long initialBackoffMs;
        public final long maxBackoffMs;
        public final double backoffMultiplier;
        public final boolean jitterEnabled;
        public final double jitterFactor;
        
        public RetryPolicy(int maxRetries, long initialBackoffMs, long maxBackoffMs,
                         double backoffMultiplier, boolean jitterEnabled, double jitterFactor) {
            this.maxRetries = maxRetries;
            this.initialBackoffMs = initialBackoffMs;
            this.maxBackoffMs = maxBackoffMs;
            this.backoffMultiplier = backoffMultiplier;
            this.jitterEnabled = jitterEnabled;
            this.jitterFactor = jitterFactor;
        }
        
        public static RetryPolicy defaultPolicy() {
            return new RetryPolicy(3, 1000, 30000, 2.0, true, 0.1);
        }
        
        public static RetryPolicy aggressivePolicy() {
            return new RetryPolicy(5, 500, 60000, 2.0, true, 0.2);
        }
        
        public static RetryPolicy conservativePolicy() {
            return new RetryPolicy(2, 2000, 10000, 1.5, false, 0.0);
        }
    }
    
    
    public enum CircuitState {
        CLOSED,
        OPEN,
        HALF_OPEN
    }
    
    
    public static class CircuitBreakerConfig {
        public final int failureThreshold;
        public final long timeoutMs;
        public final int successThreshold;
        
        public CircuitBreakerConfig(int failureThreshold, long timeoutMs, int successThreshold) {
            this.failureThreshold = failureThreshold;
            this.timeoutMs = timeoutMs;
            this.successThreshold = successThreshold;
        }
        
        public static CircuitBreakerConfig defaultConfig() {
            return new CircuitBreakerConfig(5, 60000, 2);
        }
    }
    
    
    public static class CircuitBreaker {
        public final String name;
        public volatile CircuitState state;
        public volatile int failureCount;
        public volatile int successCount;
        public volatile long lastFailureTime;
        public volatile long lastStateChangeTime;
        public final CircuitBreakerConfig config;
        
        public CircuitBreaker(String name, CircuitBreakerConfig config) {
            this.name = name;
            this.state = CircuitState.CLOSED;
            this.failureCount = 0;
            this.successCount = 0;
            this.lastFailureTime = 0;
            this.lastStateChangeTime = System.currentTimeMillis();
            this.config = config;
        }
        
        public synchronized void recordFailure() {
            failureCount++;
            lastFailureTime = System.currentTimeMillis();
            
            if (failureCount >= config.failureThreshold && state == CircuitState.CLOSED) {
                transitionTo(CircuitState.OPEN);
            }
        }
        
        public synchronized void recordSuccess() {
            successCount++;
            
            if (state == CircuitState.HALF_OPEN) {
                if (successCount >= config.successThreshold) {
                    transitionTo(CircuitState.CLOSED);
                }
            }
        }
        
        public synchronized void transitionTo(CircuitState newState) {
            state = newState;
            lastStateChangeTime = System.currentTimeMillis();
            
            if (newState == CircuitState.CLOSED) {
                failureCount = 0;
                successCount = 0;
            } else if (newState == CircuitState.HALF_OPEN) {
                successCount = 0;
            }
        }
        
        public synchronized boolean allowRequest() {
            if (state == CircuitState.CLOSED) {
                return true;
            }
            
            if (state == CircuitState.OPEN) {
                long timeSinceFailure = System.currentTimeMillis() - lastFailureTime;
                if (timeSinceFailure >= config.timeoutMs) {
                    transitionTo(CircuitState.HALF_OPEN);
                    return true;
                }
                return false;
            }
            
            if (state == CircuitState.HALF_OPEN) {
                return true;
            }
            
            return false;
        }
        
        public synchronized void reset() {
            transitionTo(CircuitState.CLOSED);
        }
    }
    
    
    public static class RetryAttempt {
        public final int attemptNumber;
        public final long timestamp;
        public final long backoffMs;
        public final boolean success;
        public final String error;
        
        public RetryAttempt(int attemptNumber, long backoffMs, boolean success, String error) {
            this.attemptNumber = attemptNumber;
            this.timestamp = System.currentTimeMillis();
            this.backoffMs = backoffMs;
            this.success = success;
            this.error = error;
        }
    }
    
    
    public interface RetryOperation<T> {
        T execute() throws Exception;
    }
    
    
    private volatile RetryPolicy defaultPolicy;
    
    
    private final Map<String, CircuitBreaker> circuitBreakers;
    
    
    private final Map<String, List<RetryAttempt>> retryHistory;
    
    
    private final Map<String, AtomicInteger> retryCount;
    private final Map<String, AtomicInteger> successCount;
    private final Map<String, AtomicInteger> failureCount;
    private final Map<String, AtomicLong> totalRetryTime;
    
    
    private final ReentrantLock circuitLock;
    
    


    public RetryEngine() {
        this.defaultPolicy = RetryPolicy.defaultPolicy();
        this.circuitBreakers = new ConcurrentHashMap<>();
        this.retryHistory = new ConcurrentHashMap<>();
        this.retryCount = new ConcurrentHashMap<>();
        this.successCount = new ConcurrentHashMap<>();
        this.failureCount = new ConcurrentHashMap<>();
        this.totalRetryTime = new ConcurrentHashMap<>();
        this.circuitLock = new ReentrantLock();
    }
    
    


    public void setDefaultPolicy(RetryPolicy policy) {
        this.defaultPolicy = policy;
    }
    
    


    public <T> T executeWithRetry(String operationName, RetryOperation<T> operation) throws Exception {
        return executeWithRetry(operationName, operation, defaultPolicy);
    }
    
    


    public <T> T executeWithRetry(String operationName, RetryOperation<T> operation, RetryPolicy policy) throws Exception {
        
        CircuitBreaker breaker = circuitBreakers.get(operationName);
        if (breaker != null && !breaker.allowRequest()) {
            throw new Exception("Circuit breaker is OPEN for operation: " + operationName);
        }
        
        List<RetryAttempt> attempts = new ArrayList<>();
        int attemptNumber = 0;
        Exception lastException = null;
        long totalStartTime = System.currentTimeMillis();
        
        while (attemptNumber <= policy.maxRetries) {
            attemptNumber++;
            
            try {
                T result = operation.execute();
                
                
                recordSuccess(operationName, attemptNumber, 0, true, null);
                attempts.add(new RetryAttempt(attemptNumber, 0, true, null));
                
                
                if (breaker != null) {
                    breaker.recordSuccess();
                }
                
                
                retryHistory.put(operationName, attempts);
                
                return result;
                
            } catch (Exception e) {
                lastException = e;
                
                
                long backoff = calculateBackoff(attemptNumber, policy);
                recordFailure(operationName, attemptNumber, backoff, false, e.getMessage());
                attempts.add(new RetryAttempt(attemptNumber, backoff, false, e.getMessage()));
                
                
                if (breaker != null) {
                    breaker.recordFailure();
                }
                
                
                if (attemptNumber <= policy.maxRetries) {
                    if (backoff > 0) {
                        Thread.sleep(backoff);
                    }
                }
            }
        }
        
        
        retryHistory.put(operationName, attempts);
        
        
        long totalRetryTime = System.currentTimeMillis() - totalStartTime;
        this.totalRetryTime.computeIfAbsent(operationName, k -> new AtomicLong(0)).addAndGet(totalRetryTime);
        
        throw lastException;
    }
    
    


    private long calculateBackoff(int attemptNumber, RetryPolicy policy) {
        if (attemptNumber == 1) {
            return 0;
        }
        
        long backoff = (long) (policy.initialBackoffMs * Math.pow(policy.backoffMultiplier, attemptNumber - 2));
        backoff = Math.min(backoff, policy.maxBackoffMs);
        
        if (policy.jitterEnabled) {
            double jitter = backoff * policy.jitterFactor * (Math.random() * 2 - 1);
            backoff = (long) (backoff + jitter);
            backoff = Math.max(0, backoff);
        }
        
        return backoff;
    }
    
    


    private void recordSuccess(String operationName, int attemptNumber, long backoff, boolean success, String error) {
        retryCount.computeIfAbsent(operationName, k -> new AtomicInteger(0)).incrementAndGet();
        successCount.computeIfAbsent(operationName, k -> new AtomicInteger(0)).incrementAndGet();
        
        
        
        CircuitBreaker breaker = circuitBreakers.get(operationName);
        if (breaker != null) {
            
            breaker.recordSuccess();
        }
    }
    
    


    private void recordFailure(String operationName, int attemptNumber, long backoff, boolean success, String error) {
        retryCount.computeIfAbsent(operationName, k -> new AtomicInteger(0)).incrementAndGet();
        failureCount.computeIfAbsent(operationName, k -> new AtomicInteger(0)).incrementAndGet();
        
        
        
        List<RetryAttempt> history = retryHistory.get(operationName);
        if (history != null) {
            
            history.add(new RetryAttempt(attemptNumber, backoff, success, error));
        }
    }
    
    


    public CircuitBreaker getCircuitBreaker(String name) {
        return circuitBreakers.computeIfAbsent(name, 
            k -> new CircuitBreaker(k, CircuitBreakerConfig.defaultConfig()));
    }
    
    


    public CircuitBreaker createCircuitBreaker(String name, CircuitBreakerConfig config) {
        circuitLock.lock();
        try {
            CircuitBreaker breaker = new CircuitBreaker(name, config);
            circuitBreakers.put(name, breaker);
            return breaker;
        } finally {
            circuitLock.unlock();
        }
    }
    
    


    public void resetCircuitBreaker(String name) {
        CircuitBreaker breaker = circuitBreakers.get(name);
        if (breaker != null) {
            breaker.reset();
        }
    }
    
    


    public List<RetryAttempt> getRetryHistory(String operationName) {
        List<RetryAttempt> history = retryHistory.get(operationName);
        if (history == null) {
            return new ArrayList<>();
        }
        return new ArrayList<>(history);
    }
    
    


    public RetryStats getStats(String operationName) {
        return new RetryStats(
            retryCount.getOrDefault(operationName, new AtomicInteger(0)).get(),
            successCount.getOrDefault(operationName, new AtomicInteger(0)).get(),
            failureCount.getOrDefault(operationName, new AtomicInteger(0)).get(),
            totalRetryTime.getOrDefault(operationName, new AtomicLong(0)).get()
        );
    }
    
    


    public Map<String, RetryStats> getAllStats() {
        Map<String, RetryStats> stats = new HashMap<>();
        for (String operationName : retryCount.keySet()) {
            stats.put(operationName, getStats(operationName));
        }
        return stats;
    }
    
    


    public void resetStats(String operationName) {
        retryCount.remove(operationName);
        successCount.remove(operationName);
        failureCount.remove(operationName);
        totalRetryTime.remove(operationName);
        retryHistory.remove(operationName);
    }
    
    


    public void resetAllStats() {
        retryCount.clear();
        successCount.clear();
        failureCount.clear();
        totalRetryTime.clear();
        retryHistory.clear();
    }
    
    


    public void clearCircuitBreakers() {
        circuitBreakers.clear();
    }
    
    


    public static class RetryStats {
        public final int totalRetries;
        public final int totalSuccesses;
        public final int totalFailures;
        public final long totalRetryTime;
        
        public RetryStats(int totalRetries, int totalSuccesses, int totalFailures, long totalRetryTime) {
            this.totalRetries = totalRetries;
            this.totalSuccesses = totalSuccesses;
            this.totalFailures = totalFailures;
            this.totalRetryTime = totalRetryTime;
        }
        
        public double getSuccessRate() {
            if (totalRetries == 0) {
                return 0.0;
            }
            return (double) totalSuccesses / totalRetries;
        }
        
        public double getAverageRetryTime() {
            if (totalRetries == 0) {
                return 0.0;
            }
            return (double) totalRetryTime / totalRetries;
        }
    }
}
