package com.aetherflow;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;
import java.util.function.*;






public class LoadBalancer {
    
    
    public enum LoadBalancingAlgorithm {
        ROUND_ROBIN,
        LEAST_CONNECTIONS,
        WEIGHTED_ROUND_ROBIN,
        IP_HASH,
        RANDOM,
        LEAST_RESPONSE_TIME,
        CONSISTENT_HASH,
        CUSTOM
    }
    
    
    public static class ServerEndpoint {
        public final String serverId;
        public final String host;
        public final int port;
        public final int weight;
        public volatile boolean healthy;
        public volatile long lastHealthCheck;
        public volatile int currentConnections;
        public volatile long totalConnections;
        public volatile long totalRequests;
        public volatile long failedRequests;
        public volatile double averageResponseTime;
        public final Map<String, Object> metadata;
        public final ReentrantLock endpointLock;
        
        public ServerEndpoint(String serverId, String host, int port, int weight, 
                            Map<String, Object> metadata) {
            this.serverId = serverId;
            this.host = host;
            this.port = port;
            this.weight = weight;
            this.healthy = true;
            this.lastHealthCheck = System.currentTimeMillis();
            this.currentConnections = 0;
            this.totalConnections = 0;
            this.totalRequests = 0;
            this.failedRequests = 0;
            this.averageResponseTime = 0;
            this.metadata = metadata != null ? new HashMap<>(metadata) : new HashMap<>();
            this.endpointLock = new ReentrantLock();
        }
        
        public String getAddress() {
            return host + ":" + port;
        }
        
        public double getFailureRate() {
            if (totalRequests == 0) {
                return 0;
            }
            return (double) failedRequests / totalRequests;
        }
        
        public void recordRequest(boolean success, long responseTime) {
            totalRequests++;
            if (success) {
                
                double currentAvg = averageResponseTime;
                long total = totalRequests;
                double newAvg = (currentAvg * (total - 1) + responseTime) / total;
                averageResponseTime = newAvg;
            } else {
                failedRequests++;
            }
        }
    }
    
    
    public static class LoadBalancerConfig {
        public LoadBalancingAlgorithm algorithm;
        public int healthCheckInterval;
        public int healthCheckTimeout;
        public int maxRetries;
        public long retryDelay;
        public boolean enableSessionPersistence;
        public boolean enableHealthChecks;
        public int unhealthyThreshold;
        public int healthyThreshold;
        public boolean enableCircuitBreaker;
        public int circuitBreakerThreshold;
        public long circuitBreakerTimeout;
        public boolean enableMetrics;
        
        public LoadBalancerConfig() {
            this.algorithm = LoadBalancingAlgorithm.ROUND_ROBIN;
            this.healthCheckInterval = 30000; 
            this.healthCheckTimeout = 5000; 
            this.maxRetries = 3;
            this.retryDelay = 100; 
            this.enableSessionPersistence = true;
            this.enableHealthChecks = true;
            this.unhealthyThreshold = 3;
            this.healthyThreshold = 2;
            this.enableCircuitBreaker = true;
            this.circuitBreakerThreshold = 5;
            this.circuitBreakerTimeout = 60000; 
            this.enableMetrics = true;
        }
    }
    
    
    public static class LoadBalancerStats {
        public final AtomicLong totalRequests;
        public final AtomicLong successfulRequests;
        public final AtomicLong failedRequests;
        public final AtomicLong totalRetries;
        public final Map<String, AtomicLong> serverRequestCounts;
        public final Map<String, AtomicLong> serverFailureCounts;
        public final Map<String, Double> serverResponseTimes;
        public final AtomicLong circuitBreakerTrips;
        
        public LoadBalancerStats() {
            this.totalRequests = new AtomicLong(0);
            this.successfulRequests = new AtomicLong(0);
            this.failedRequests = new AtomicLong(0);
            this.totalRetries = new AtomicLong(0);
            this.serverRequestCounts = new ConcurrentHashMap<>();
            this.serverFailureCounts = new ConcurrentHashMap<>();
            this.serverResponseTimes = new ConcurrentHashMap<>();
            this.circuitBreakerTrips = new AtomicLong(0);
        }
        
        public void recordRequest(String serverId, boolean success, long responseTime) {
            totalRequests.incrementAndGet();
            if (success) {
                successfulRequests.incrementAndGet();
            } else {
                failedRequests.incrementAndGet();
            }
            serverRequestCounts.computeIfAbsent(serverId, k -> new AtomicLong(0)).incrementAndGet();
            
            if (!success) {
                serverFailureCounts.computeIfAbsent(serverId, k -> new AtomicLong(0)).incrementAndGet();
            }
            
            
            serverResponseTimes.compute(serverId, (k, v) -> {
                if (v == null) {
                    return (double) responseTime;
                }
                return (v + responseTime) / 2;
            });
        }
        
        public void recordRetry() {
            totalRetries.incrementAndGet();
        }
        
        public void recordCircuitBreakerTrip() {
            circuitBreakerTrips.incrementAndGet();
        }
    }
    
    
    private static class CircuitBreakerState {
        public final String serverId;
        public volatile boolean tripped;
        public volatile long tripTime;
        public volatile int consecutiveFailures;
        
        public CircuitBreakerState(String serverId) {
            this.serverId = serverId;
            this.tripped = false;
            this.tripTime = 0;
            this.consecutiveFailures = 0;
        }
        
        public void recordFailure() {
            consecutiveFailures++;
        }
        
        public void recordSuccess() {
            consecutiveFailures = 0;
            tripped = false;
        }
        
        public void trip() {
            tripped = true;
            tripTime = System.currentTimeMillis();
        }
        
        public boolean shouldReset(long timeout) {
            return tripped && (System.currentTimeMillis() - tripTime) > timeout;
        }
    }
    
    
    private final LoadBalancerConfig config;
    
    
    private final List<ServerEndpoint> servers;
    
    
    private final AtomicInteger roundRobinIndex;
    
    
    private final Map<String, String> sessionToServerMap;
    
    
    private final Map<String, CircuitBreakerState> circuitBreakers;
    
    
    private final LoadBalancerStats stats;
    
    
    private final ReentrantLock serverLock;
    
    
    private final ScheduledExecutorService healthCheckExecutor;
    
    
    private volatile boolean shutdown;
    
    
    private volatile Function<String, ServerEndpoint> customLoadBalancer;
    
    


    public LoadBalancer(LoadBalancerConfig config) {
        this.config = config;
        this.servers = new CopyOnWriteArrayList<>();
        this.roundRobinIndex = new AtomicInteger(0);
        this.sessionToServerMap = new ConcurrentHashMap<>();
        this.circuitBreakers = new ConcurrentHashMap<>();
        this.stats = new LoadBalancerStats();
        this.serverLock = new ReentrantLock();
        this.healthCheckExecutor = Executors.newSingleThreadScheduledExecutor();
        this.shutdown = false;
        
        
        if (config.enableHealthChecks) {
            startHealthCheckThread();
        }
    }
    
    


    public LoadBalancer() {
        this(new LoadBalancerConfig());
    }
    
    


    private void startHealthCheckThread() {
        healthCheckExecutor.scheduleAtFixedRate(() -> {
            performHealthChecks();
        }, config.healthCheckInterval, config.healthCheckInterval, TimeUnit.MILLISECONDS);
    }
    
    


    public void addServer(ServerEndpoint server) {
        serverLock.lock();
        try {
            servers.add(server);
            circuitBreakers.put(server.serverId, new CircuitBreakerState(server.serverId));
        } finally {
            serverLock.unlock();
        }
    }
    
    


    public void removeServer(String serverId) {
        serverLock.lock();
        try {
            servers.removeIf(server -> server.serverId.equals(serverId));
            circuitBreakers.remove(serverId);
            
            
            sessionToServerMap.entrySet().removeIf(entry -> entry.getValue().equals(serverId));
        } finally {
            serverLock.unlock();
        }
    }
    
    


    public ServerEndpoint getServer(String serverId) {
        for (ServerEndpoint server : servers) {
            if (server.serverId.equals(serverId)) {
                return server;
            }
        }
        return null;
    }
    
    


    public List<ServerEndpoint> getServers() {
        return new ArrayList<>(servers);
    }
    
    


    public List<ServerEndpoint> getHealthyServers() {
        List<ServerEndpoint> healthyServers = new ArrayList<>();
        for (ServerEndpoint server : servers) {
            if (server.healthy) {
                healthyServers.add(server);
            }
        }
        return healthyServers;
    }
    
    


    public ServerEndpoint selectServer() {
        return selectServer(null);
    }
    
    


    public ServerEndpoint selectServer(String sessionId) {
        
        if (config.enableSessionPersistence && sessionId != null) {
            String serverId = sessionToServerMap.get(sessionId);
            if (serverId != null) {
                ServerEndpoint server = getServer(serverId);
                if (server != null && server.healthy) {
                    return server;
                }
            }
        }
        
        
        ServerEndpoint selectedServer;
        switch (config.algorithm) {
            case ROUND_ROBIN:
                selectedServer = selectRoundRobin();
                break;
            case LEAST_CONNECTIONS:
                selectedServer = selectLeastConnections();
                break;
            case WEIGHTED_ROUND_ROBIN:
                selectedServer = selectWeightedRoundRobin();
                break;
            case IP_HASH:
                selectedServer = selectIpHash(sessionId);
                break;
            case RANDOM:
                selectedServer = selectRandom();
                break;
            case LEAST_RESPONSE_TIME:
                selectedServer = selectLeastResponseTime();
                break;
            case CONSISTENT_HASH:
                selectedServer = selectConsistentHash(sessionId);
                break;
            case CUSTOM:
                selectedServer = customLoadBalancer != null ? 
                    customLoadBalancer.apply(sessionId) : selectRoundRobin();
                break;
            default:
                selectedServer = selectRoundRobin();
        }
        
        
        if (config.enableSessionPersistence && sessionId != null && selectedServer != null) {
            sessionToServerMap.put(sessionId, selectedServer.serverId);
        }
        
        return selectedServer;
    }
    
    


    private ServerEndpoint selectRoundRobin() {
        List<ServerEndpoint> healthyServers = getHealthyServers();
        if (healthyServers.isEmpty()) {
            return null;
        }
        
        int index = roundRobinIndex.getAndIncrement() % healthyServers.size();
        return healthyServers.get(index);
    }
    
    


    private ServerEndpoint selectLeastConnections() {
        List<ServerEndpoint> healthyServers = getHealthyServers();
        if (healthyServers.isEmpty()) {
            return null;
        }
        
        ServerEndpoint selected = null;
        int minConnections = Integer.MAX_VALUE;
        
        for (ServerEndpoint server : healthyServers) {
            if (server.currentConnections < minConnections) {
                minConnections = server.currentConnections;
                selected = server;
            }
        }
        
        return selected;
    }
    
    


    private ServerEndpoint selectWeightedRoundRobin() {
        List<ServerEndpoint> healthyServers = getHealthyServers();
        if (healthyServers.isEmpty()) {
            return null;
        }
        
        int totalWeight = healthyServers.stream().mapToInt(s -> s.weight).sum();
        int randomWeight = ThreadLocalRandom.current().nextInt(totalWeight);
        
        int currentWeight = 0;
        for (ServerEndpoint server : healthyServers) {
            currentWeight += server.weight;
            if (randomWeight < currentWeight) {
                return server;
            }
        }
        
        return healthyServers.get(0);
    }
    
    


    private ServerEndpoint selectIpHash(String sessionId) {
        List<ServerEndpoint> healthyServers = getHealthyServers();
        if (healthyServers.isEmpty()) {
            return null;
        }
        
        if (sessionId == null) {
            return selectRandom();
        }
        
        int hash = sessionId.hashCode();
        int index = Math.abs(hash) % healthyServers.size();
        return healthyServers.get(index);
    }
    
    


    private ServerEndpoint selectRandom() {
        List<ServerEndpoint> healthyServers = getHealthyServers();
        if (healthyServers.isEmpty()) {
            return null;
        }
        
        int index = ThreadLocalRandom.current().nextInt(healthyServers.size());
        return healthyServers.get(index);
    }
    
    


    private ServerEndpoint selectLeastResponseTime() {
        List<ServerEndpoint> healthyServers = getHealthyServers();
        if (healthyServers.isEmpty()) {
            return null;
        }
        
        ServerEndpoint selected = null;
        double minResponseTime = Double.MAX_VALUE;
        
        for (ServerEndpoint server : healthyServers) {
            if (server.averageResponseTime < minResponseTime) {
                minResponseTime = server.averageResponseTime;
                selected = server;
            }
        }
        
        return selected;
    }
    
    


    private ServerEndpoint selectConsistentHash(String sessionId) {
        List<ServerEndpoint> healthyServers = getHealthyServers();
        if (healthyServers.isEmpty()) {
            return null;
        }
        
        if (sessionId == null) {
            return selectRandom();
        }
        
        
        int hash = sessionId.hashCode();
        int index = Math.abs(hash) % healthyServers.size();
        return healthyServers.get(index);
    }
    
    


    private void performHealthChecks() {
        for (ServerEndpoint server : servers) {
            boolean healthy = checkServerHealth(server);
            
            server.endpointLock.lock();
            try {
                server.healthy = healthy;
                server.lastHealthCheck = System.currentTimeMillis();
                
                
                CircuitBreakerState cbState = circuitBreakers.get(server.serverId);
                if (cbState != null) {
                    if (healthy) {
                        cbState.recordSuccess();
                    } else {
                        cbState.recordFailure();
                        
                        if (cbState.consecutiveFailures >= config.circuitBreakerThreshold) {
                            cbState.trip();
                            stats.recordCircuitBreakerTrip();
                        }
                    }
                    
                    
                    if (cbState.shouldReset(config.circuitBreakerTimeout)) {
                        cbState.tripped = false;
                        cbState.consecutiveFailures = 0;
                    }
                }
            } finally {
                server.endpointLock.unlock();
            }
        }
    }
    
    


    private boolean checkServerHealth(ServerEndpoint server) {
        
        
        
        return true;
    }
    
    


    public void recordRequest(String serverId, boolean success, long responseTime) {
        ServerEndpoint server = getServer(serverId);
        if (server != null) {
            server.endpointLock.lock();
            try {
                server.recordRequest(success, responseTime);
            } finally {
                server.endpointLock.unlock();
            }
        }
        
        stats.recordRequest(serverId, success, responseTime);
    }
    
    


    public void setCustomLoadBalancer(Function<String, ServerEndpoint> loadBalancer) {
        this.customLoadBalancer = loadBalancer;
    }
    
    


    public LoadBalancerStats getStats() {
        return stats;
    }
    
    


    public LoadBalancerConfig getConfig() {
        return config;
    }
    
    


    public Map<String, String> getSessionToServerMap() {
        return new HashMap<>(sessionToServerMap);
    }
    
    


    public void clearSessionMapping() {
        sessionToServerMap.clear();
    }
    
    


    public void removeSession(String sessionId) {
        sessionToServerMap.remove(sessionId);
    }
    
    


    public CircuitBreakerState getCircuitBreakerState(String serverId) {
        return circuitBreakers.get(serverId);
    }
    
    


    public void resetCircuitBreaker(String serverId) {
        CircuitBreakerState state = circuitBreakers.get(serverId);
        if (state != null) {
            state.tripped = false;
            state.consecutiveFailures = 0;
        }
    }
    
    


    public void shutdown() {
        shutdown = true;
        
        healthCheckExecutor.shutdown();
        try {
            healthCheckExecutor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        servers.clear();
        sessionToServerMap.clear();
        circuitBreakers.clear();
    }
}
