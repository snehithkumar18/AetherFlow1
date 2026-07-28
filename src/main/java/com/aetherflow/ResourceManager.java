package com.aetherflow;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;
import java.io.*;






public class ResourceManager {
    
    
    public enum ResourceType {
        MEMORY,
        CPU,
        FILE_HANDLE,
        NETWORK_CONNECTION,
        THREAD,
        SOCKET,
        BUFFER,
        CUSTOM
    }
    
    
    public static class ResourceAllocation {
        public final String allocationId;
        public final ResourceType type;
        public final long amount;
        public final long timestamp;
        public final String owner;
        public final Map<String, Object> metadata;
        public volatile boolean active;
        public volatile long lastUsed;
        public final ReentrantLock allocationLock;
        
        public ResourceAllocation(String allocationId, ResourceType type, long amount, 
                                  String owner, Map<String, Object> metadata) {
            this.allocationId = allocationId;
            this.type = type;
            this.amount = amount;
            this.timestamp = System.currentTimeMillis();
            this.owner = owner;
            this.metadata = metadata != null ? new HashMap<>(metadata) : new HashMap<>();
            this.active = true;
            this.lastUsed = timestamp;
            this.allocationLock = new ReentrantLock();
        }
        
        public long getAge() {
            return System.currentTimeMillis() - timestamp;
        }
        
        public long getIdleTime() {
            return System.currentTimeMillis() - lastUsed;
        }
        
        public void recordUsage() {
            lastUsed = System.currentTimeMillis();
        }
    }
    
    
    public static class ResourcePool {
        public final ResourceType type;
        public final long totalCapacity;
        public final AtomicLong allocated;
        public final AtomicLong available;
        public final AtomicLong peakUsage;
        public final long allocationTimeout;
        public final boolean enableQuota;
        public final long quotaPerOwner;
        public final Map<String, AtomicLong> ownerUsage;
        public final ReentrantLock poolLock;
        
        public ResourcePool(ResourceType type, long totalCapacity, long allocationTimeout,
                          boolean enableQuota, long quotaPerOwner) {
            this.type = type;
            this.totalCapacity = totalCapacity;
            this.allocated = new AtomicLong(0);
            this.available = new AtomicLong(totalCapacity);
            this.peakUsage = new AtomicLong(0);
            this.allocationTimeout = allocationTimeout;
            this.enableQuota = enableQuota;
            this.quotaPerOwner = quotaPerOwner;
            this.ownerUsage = new ConcurrentHashMap<>();
            this.poolLock = new ReentrantLock();
        }
        
        public double getUtilization() {
            return (double) allocated.get() / totalCapacity;
        }
        
        public boolean hasCapacity(long amount) {
            if (amount < 0) {
                amount = Math.abs(amount);
            }
            return available.get() >= amount;
        }
        
        public boolean canAllocate(String owner, long amount) {
            if (!hasCapacity(amount)) {
                return false;
            }
            
            if (enableQuota) {
                AtomicLong ownerAllocated = ownerUsage.get(owner);
                long currentUsage = ownerAllocated != null ? ownerAllocated.get() : 0;
                return currentUsage + amount <= quotaPerOwner;
            }
            
            return true;
        }
    }
    
    
    public static class ResourceManagerConfig {
        public long memoryCapacity;
        public long cpuCapacity;
        public int fileHandleCapacity;
        public int networkConnectionCapacity;
        public int threadCapacity;
        public int socketCapacity;
        public long bufferCapacity;
        public boolean enableQuotas;
        public long defaultQuota;
        public long allocationTimeout;
        public boolean enableMonitoring;
        public long monitoringInterval;
        public boolean enableAutoCleanup;
        public long cleanupInterval;
        
        public ResourceManagerConfig() {
            this.memoryCapacity = Runtime.getRuntime().maxMemory();
            this.cpuCapacity = 100; 
            this.fileHandleCapacity = 10000;
            this.networkConnectionCapacity = 1000;
            this.threadCapacity = 500;
            this.socketCapacity = 1000;
            this.bufferCapacity = 100 * 1024 * 1024; 
            this.enableQuotas = true;
            this.defaultQuota = memoryCapacity / 10; 
            this.allocationTimeout = 30000; 
            this.enableMonitoring = true;
            this.monitoringInterval = 5000; 
            this.enableAutoCleanup = true;
            this.cleanupInterval = 60000; 
        }
    }
    
    
    public static class ResourceManagerStats {
        public final AtomicLong totalAllocations;
        public final AtomicLong totalDeallocations;
        public final AtomicLong totalAllocationFailures;
        public final AtomicLong totalQuotaExceeded;
        public final AtomicLong totalTimeouts;
        public final AtomicLong currentAllocations;
        public final Map<String, AtomicLong> typeAllocations;
        public final Map<String, AtomicLong> ownerAllocations;
        public final Map<String, AtomicLong> errorCounts;
        
        public ResourceManagerStats() {
            this.totalAllocations = new AtomicLong(0);
            this.totalDeallocations = new AtomicLong(0);
            this.totalAllocationFailures = new AtomicLong(0);
            this.totalQuotaExceeded = new AtomicLong(0);
            this.totalTimeouts = new AtomicLong(0);
            this.currentAllocations = new AtomicLong(0);
            this.typeAllocations = new ConcurrentHashMap<>();
            this.ownerAllocations = new ConcurrentHashMap<>();
            this.errorCounts = new ConcurrentHashMap<>();
        }
        
        public void recordAllocation(ResourceType type, String owner) {
            totalAllocations.incrementAndGet();
            currentAllocations.incrementAndGet();
            typeAllocations.computeIfAbsent(type.name(), k -> new AtomicLong(0)).incrementAndGet();
            ownerAllocations.computeIfAbsent(owner, k -> new AtomicLong(0)).incrementAndGet();
        }
        
        public void recordDeallocation(ResourceType type, String owner) {
            totalDeallocations.incrementAndGet();
            currentAllocations.decrementAndGet();
            typeAllocations.computeIfAbsent(type.name(), k -> new AtomicLong(0)).decrementAndGet();
            ownerAllocations.computeIfAbsent(owner, k -> new AtomicLong(0)).decrementAndGet();
        }
        
        public void recordAllocationFailure(String errorType) {
            totalAllocationFailures.incrementAndGet();
            errorCounts.computeIfAbsent(errorType, k -> new AtomicLong(0)).incrementAndGet();
        }
        
        public void recordQuotaExceeded() {
            totalQuotaExceeded.incrementAndGet();
        }
        
        public void recordTimeout() {
            totalTimeouts.incrementAndGet();
        }
    }
    
    
    private final ResourceManagerConfig config;
    
    
    private final Map<ResourceType, ResourcePool> resourcePools;
    
    
    private final Map<String, ResourceAllocation> allocations;
    
    
    private final AtomicLong allocationIdGenerator;
    
    
    private final ResourceManagerStats stats;
    
    
    private final ReentrantLock allocationLock;
    
    
    private final ScheduledExecutorService scheduledExecutor;
    
    
    private volatile boolean shutdown;
    
    


    public ResourceManager(ResourceManagerConfig config) {
        this.config = config;
        this.resourcePools = new ConcurrentHashMap<>();
        this.allocations = new ConcurrentHashMap<>();
        this.allocationIdGenerator = new AtomicLong(0);
        this.stats = new ResourceManagerStats();
        this.allocationLock = new ReentrantLock();
        this.scheduledExecutor = Executors.newScheduledThreadPool(4);
        this.shutdown = false;
        
        
        initializeResourcePools();
        
        
        if (config.enableMonitoring) {
            startMonitoringThread();
        }
        
        if (config.enableAutoCleanup) {
            startCleanupThread();
        }
    }
    
    


    public ResourceManager() {
        this(new ResourceManagerConfig());
    }
    
    


    private void initializeResourcePools() {
        resourcePools.put(ResourceType.MEMORY, 
            new ResourcePool(ResourceType.MEMORY, config.memoryCapacity, 
                          config.allocationTimeout, config.enableQuotas, config.defaultQuota));
        
        resourcePools.put(ResourceType.CPU,
            new ResourcePool(ResourceType.CPU, config.cpuCapacity,
                          config.allocationTimeout, false, 0));
        
        resourcePools.put(ResourceType.FILE_HANDLE,
            new ResourcePool(ResourceType.FILE_HANDLE, config.fileHandleCapacity,
                          config.allocationTimeout, config.enableQuotas, config.defaultQuota));
        
        resourcePools.put(ResourceType.NETWORK_CONNECTION,
            new ResourcePool(ResourceType.NETWORK_CONNECTION, config.networkConnectionCapacity,
                          config.allocationTimeout, config.enableQuotas, config.defaultQuota));
        
        resourcePools.put(ResourceType.THREAD,
            new ResourcePool(ResourceType.THREAD, config.threadCapacity,
                          config.allocationTimeout, config.enableQuotas, config.defaultQuota));
        
        resourcePools.put(ResourceType.SOCKET,
            new ResourcePool(ResourceType.SOCKET, config.socketCapacity,
                          config.allocationTimeout, config.enableQuotas, config.defaultQuota));
        
        resourcePools.put(ResourceType.BUFFER,
            new ResourcePool(ResourceType.BUFFER, config.bufferCapacity,
                          config.allocationTimeout, config.enableQuotas, config.defaultQuota));
    }
    
    


    private void startMonitoringThread() {
        scheduledExecutor.scheduleAtFixedRate(() -> {
            monitorResources();
        }, config.monitoringInterval, config.monitoringInterval, TimeUnit.MILLISECONDS);
    }
    
    


    private void startCleanupThread() {
        scheduledExecutor.scheduleAtFixedRate(() -> {
            cleanupExpiredAllocations();
        }, config.cleanupInterval, config.cleanupInterval, TimeUnit.MILLISECONDS);
    }
    
    


    public ResourceAllocation allocate(ResourceType type, long amount, String owner) {
        return allocate(type, amount, owner, null);
    }
    
    


    public ResourceAllocation allocate(ResourceType type, long amount, String owner, 
                                      Map<String, Object> metadata) {
        ResourcePool pool = resourcePools.get(type);
        if (pool == null) {
            stats.recordAllocationFailure("Unknown resource type");
            throw new IllegalArgumentException("Unknown resource type: " + type);
        }
        
        String allocationId = "alloc_" + allocationIdGenerator.incrementAndGet() + "_" + 
                             System.currentTimeMillis();
        
        allocationLock.lock();
        try {
            
            if (!pool.canAllocate(owner, amount)) {
                if (!pool.hasCapacity(amount)) {
                    stats.recordAllocationFailure("Insufficient capacity");
                } else {
                    stats.recordQuotaExceeded();
                    stats.recordAllocationFailure("Quota exceeded");
                }
                throw new ResourceAllocationException("Cannot allocate resource: " + type);
            }
            
            
            
            long currentAvailable = pool.available.get();
            if (currentAvailable < amount) {
                stats.recordAllocationFailure("Race condition detected");
                throw new ResourceAllocationException("Capacity exceeded due to race condition");
            }
            
            
            pool.allocated.addAndGet(amount);
            pool.available.addAndGet(-amount);
            
            
            long currentAllocated = pool.allocated.get();
            if (currentAllocated > pool.peakUsage.get()) {
                pool.peakUsage.set(currentAllocated);
            }
            
            
            if (pool.enableQuota) {
                pool.ownerUsage.computeIfAbsent(owner, k -> new AtomicLong(0))
                              .addAndGet(amount);
            }
            
            
            ResourceAllocation allocation = new ResourceAllocation(allocationId, type, amount, 
                                                                   owner, metadata);
            allocations.put(allocationId, allocation);
            
            stats.recordAllocation(type, owner);
            
            return allocation;
            
        } finally {
            allocationLock.unlock();
        }
    }
    
    


    public void deallocate(String allocationId) {
        allocationLock.lock();
        try {
            ResourceAllocation allocation = allocations.remove(allocationId);
            if (allocation == null) {
                return;
            }
            
            allocation.active = false;
            
            ResourcePool pool = resourcePools.get(allocation.type);
            if (pool != null) {
                pool.allocated.addAndGet(-allocation.amount);
                pool.available.addAndGet(allocation.amount);
                
                
                if (pool.enableQuota) {
                    AtomicLong ownerAllocated = pool.ownerUsage.get(allocation.owner);
                    if (ownerAllocated != null) {
                        ownerAllocated.addAndGet(-allocation.amount);
                    }
                }
            }
            
            stats.recordDeallocation(allocation.type, allocation.owner);
            
        } finally {
            allocationLock.unlock();
        }
    }
    
    


    public ResourceAllocation getAllocation(String allocationId) {
        return allocations.get(allocationId);
    }
    
    


    public Collection<ResourceAllocation> getAllocations() {
        return new ArrayList<>(allocations.values());
    }
    
    


    public List<ResourceAllocation> getAllocationsByOwner(String owner) {
        List<ResourceAllocation> ownerAllocations = new ArrayList<>();
        for (ResourceAllocation allocation : allocations.values()) {
            if (allocation.owner.equals(owner)) {
                ownerAllocations.add(allocation);
            }
        }
        return ownerAllocations;
    }
    
    


    public List<ResourceAllocation> getAllocationsByType(ResourceType type) {
        List<ResourceAllocation> typeAllocations = new ArrayList<>();
        for (ResourceAllocation allocation : allocations.values()) {
            if (allocation.type == type) {
                typeAllocations.add(allocation);
            }
        }
        return typeAllocations;
    }
    
    


    public ResourcePool getResourcePool(ResourceType type) {
        return resourcePools.get(type);
    }
    
    


    public Map<ResourceType, ResourcePool> getResourcePools() {
        return new HashMap<>(resourcePools);
    }
    
    


    private void monitorResources() {
        for (ResourcePool pool : resourcePools.values()) {
            double utilization = pool.getUtilization();
            
            
            if (utilization > 0.9) {
                
            }
            
            
            for (ResourceAllocation allocation : allocations.values()) {
                if (allocation.type == pool.type && allocation.active) {
                    allocation.recordUsage();
                }
            }
        }
    }
    
    


    private void cleanupExpiredAllocations() {
        long now = System.currentTimeMillis();
        
        allocationLock.lock();
        try {
            Iterator<Map.Entry<String, ResourceAllocation>> it = allocations.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<String, ResourceAllocation> entry = it.next();
                ResourceAllocation allocation = entry.getValue();
                
                if (allocation.getIdleTime() > config.allocationTimeout) {
                    it.remove();
                    deallocate(entry.getKey());
                }
            }
        } finally {
            allocationLock.unlock();
        }
    }
    
    


    public ResourceManagerStats getStats() {
        return stats;
    }
    
    


    public ResourceManagerConfig getConfig() {
        return config;
    }
    
    


    public void setQuota(String owner, long quota) {
        for (ResourcePool pool : resourcePools.values()) {
            if (pool.enableQuota) {
                pool.ownerUsage.put(owner, new AtomicLong(0));
            }
        }
    }
    
    


    public long getQuota(String owner) {
        return config.defaultQuota;
    }
    
    


    public long getUsage(String owner) {
        long totalUsage = 0;
        for (ResourcePool pool : resourcePools.values()) {
            if (pool.enableQuota) {
                AtomicLong ownerAllocated = pool.ownerUsage.get(owner);
                if (ownerAllocated != null) {
                    totalUsage += ownerAllocated.get();
                }
            }
        }
        return totalUsage;
    }
    
    


    public void shutdown() {
        shutdown = true;
        
        
        allocationLock.lock();
        try {
            for (String allocationId : new ArrayList<>(allocations.keySet())) {
                deallocate(allocationId);
            }
        } finally {
            allocationLock.unlock();
        }
        
        
        scheduledExecutor.shutdown();
        try {
            scheduledExecutor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        resourcePools.clear();
        allocations.clear();
    }
    
    


    public static class ResourceAllocationException extends RuntimeException {
        public ResourceAllocationException(String message) {
            super(message);
        }
        
        public ResourceAllocationException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
