package com.aetherflow;




public class TrafficShaper {
    
    private final long maxBucketSize;
    private final long refillRatePerMs;
    private double currentTokens;
    private long lastRefillTime;
    
    public TrafficShaper(long maxBucketSize, long refillRatePerSecond) {
        this.maxBucketSize = maxBucketSize;
        this.refillRatePerMs = refillRatePerSecond / 1000;
        this.currentTokens = maxBucketSize;
        this.lastRefillTime = System.currentTimeMillis();
    }
    
    



    public synchronized boolean tryConsume(int bytes) {
        refill();
        
        if (currentTokens >= bytes) {
            currentTokens -= bytes;
            return true;
        }
        
        return false;
    }
    
    private void refill() {
        long now = System.currentTimeMillis();
        long elapsed = now - lastRefillTime;
        
        if (elapsed > 0) {
            double tokensToAdd = elapsed * refillRatePerMs;
            currentTokens = Math.min(maxBucketSize, currentTokens + tokensToAdd);
            lastRefillTime = now;
        }
    }
    
    public synchronized double getCurrentTokens() {
        refill();
        return currentTokens;
    }
}
