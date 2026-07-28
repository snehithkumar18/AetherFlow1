package com.aetherflow;

import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;





public class BufferPool {
    
    
    public static class BufferEntry {
        public final ByteBuffer buffer;
        public final int size;
        public volatile long lastUsed;
        public volatile boolean inUse;
        public volatile int useCount;
        public final long creationTime;
        public volatile WeakReference<BufferEntry> weakRef;
        
        public BufferEntry(int size) {
            this.size = size;
            this.buffer = ByteBuffer.allocateDirect(size);
            this.lastUsed = System.currentTimeMillis();
            this.inUse = false;
            this.useCount = 0;
            this.creationTime = System.currentTimeMillis();
            this.weakRef = new WeakReference<>(this);
        }
        
        public void acquire() {
            inUse = true;
            useCount++;
            lastUsed = System.currentTimeMillis();
            
            if (useCount == Integer.MAX_VALUE) {
                useCount = 0;
            }
        }
        
        public void release() {
            inUse = false;
            lastUsed = System.currentTimeMillis();
            
            useCount--;
        }
        
        public long getAge() {
            return System.currentTimeMillis() - creationTime;
        }
        
        public long getIdleTime() {
            return System.currentTimeMillis() - lastUsed;
        }
        
        public void clear() {
            buffer.clear();
        }
    }
    
    
    private static final int DEFAULT_BUFFER_SIZE = 8192; 
    private static final int MAX_BUFFER_SIZE = 1024 * 1024; 
    private static final int MAX_POOL_SIZE = 1000;
    private static final long BUFFER_TTL_MS = 300000; 
    private static final long CLEANUP_INTERVAL_MS = 60000; 
    
    
    private final Map<Integer, List<BufferEntry>> bufferPools;
    
    
    private final Map<Long, BufferEntry> bufferById;
    
    
    private final AtomicLong bufferIdGenerator;
    
    
    private final ReentrantLock poolLock;
    
    
    private final AtomicInteger totalAllocations;
    private final AtomicInteger totalReuses;
    private final AtomicInteger totalReleases;
    private final AtomicInteger totalEvictions;
    private final AtomicInteger totalCleanups;
    
    
    private volatile boolean underPressure;
    
    
    private volatile long lastCleanupTime;
    
    


    public BufferPool() {
        this.bufferPools = new ConcurrentHashMap<>();
        this.bufferById = new ConcurrentHashMap<>();
        this.bufferIdGenerator = new AtomicLong(0);
        this.poolLock = new ReentrantLock();
        this.totalAllocations = new AtomicInteger(0);
        this.totalReuses = new AtomicInteger(0);
        this.totalReleases = new AtomicInteger(0);
        this.totalEvictions = new AtomicInteger(0);
        this.totalCleanups = new AtomicInteger(0);
        this.underPressure = false;
        this.lastCleanupTime = System.currentTimeMillis();
    }
    
    


    public BufferEntry acquireBuffer(int size) {
        if (size <= 0 || size > MAX_BUFFER_SIZE) {
            size = DEFAULT_BUFFER_SIZE;
        }
        
        
        int roundedSize = roundToPowerOfTwo(size);
        
        poolLock.lock();
        try {
            List<BufferEntry> pool = bufferPools.computeIfAbsent(roundedSize, k -> new ArrayList<>());
            
            
            BufferEntry entry = findAvailableBuffer(pool);
            
            if (entry != null) {
                entry.acquire();
                totalReuses.incrementAndGet();
                return entry;
            }
            
            
            if (pool.size() < MAX_POOL_SIZE) {
                entry = allocateNewBuffer(roundedSize);
                entry.acquire();
                totalAllocations.incrementAndGet();
                return entry;
            }
            
            
            entry = evictBuffer(pool);
            if (entry != null) {
                entry.acquire();
                totalReuses.incrementAndGet();
                totalEvictions.incrementAndGet();
                return entry;
            }
            
            
            underPressure = true;
            entry = allocateNewBuffer(roundedSize);
            entry.acquire();
            totalAllocations.incrementAndGet();
            return entry;
            
        } finally {
            poolLock.unlock();
        }
    }
    
    


    public void releaseBuffer(BufferEntry entry) {
        if (entry == null) {
            return;
        }
        
        poolLock.lock();
        try {
            entry.release();
            totalReleases.incrementAndGet();
            
            List<BufferEntry> pool = bufferPools.computeIfAbsent(entry.size, k -> new ArrayList<>());
            
            if (!pool.contains(entry)) {
                pool.add(entry);
            }
            
            
            entry.clear();
            
            
            if (System.currentTimeMillis() - lastCleanupTime > CLEANUP_INTERVAL_MS) {
                cleanupExpiredBuffers();
            }
            
        } finally {
            poolLock.unlock();
        }
    }
    
    


    private BufferEntry findAvailableBuffer(List<BufferEntry> pool) {
        for (BufferEntry entry : pool) {
            if (!entry.inUse) {
                return entry;
            }
        }
        return null;
    }
    
    


    private BufferEntry allocateNewBuffer(int size) {
        BufferEntry entry = new BufferEntry(size);
        long bufferId = bufferIdGenerator.incrementAndGet();
        bufferById.put(bufferId, entry);
        
        List<BufferEntry> pool = bufferPools.computeIfAbsent(size, k -> new ArrayList<>());
        pool.add(entry);
        
        return entry;
    }
    
    


    private BufferEntry evictBuffer(List<BufferEntry> pool) {
        if (pool.isEmpty()) {
            return null;
        }
        
        BufferEntry oldest = null;
        long oldestIdleTime = 0;
        
        for (BufferEntry entry : pool) {
            if (!entry.inUse) {
                long idleTime = entry.getIdleTime();
                if (idleTime > oldestIdleTime) {
                    oldestIdleTime = idleTime;
                    oldest = entry;
                }
            }
        }
        
        if (oldest != null) {
            pool.remove(oldest);
        }
        
        return oldest;
    }
    
    


    public int cleanupExpiredBuffers() {
        int cleaned = 0;
        
        poolLock.lock();
        try {
            for (Map.Entry<Integer, List<BufferEntry>> entry : bufferPools.entrySet()) {
                List<BufferEntry> pool = entry.getValue();
                List<BufferEntry> toRemove = new ArrayList<>();
                
                for (BufferEntry buffer : pool) {
                    if (!buffer.inUse && buffer.getIdleTime() > BUFFER_TTL_MS) {
                        toRemove.add(buffer);
                    }
                }
                
                for (BufferEntry buffer : toRemove) {
                    pool.remove(buffer);
                    bufferById.remove(buffer);
                    cleaned++;
                }
            }
            
            totalCleanups.incrementAndGet();
            lastCleanupTime = System.currentTimeMillis();
            
        } finally {
            poolLock.unlock();
        }
        
        return cleaned;
    }
    
    


    public BufferEntry getBufferById(long bufferId) {
        return bufferById.get(bufferId);
    }
    
    


    private int roundToPowerOfTwo(int size) {
        int power = 1;
        while (power < size && power < MAX_BUFFER_SIZE) {
            power <<= 1;
        }
        return power;
    }
    
    


    public PoolStats getStats() {
        int totalBuffers = 0;
        int inUseBuffers = 0;
        long totalMemory = 0;
        
        poolLock.lock();
        try {
            for (List<BufferEntry> pool : bufferPools.values()) {
                for (BufferEntry entry : pool) {
                    totalBuffers++;
                    if (entry.inUse) {
                        inUseBuffers++;
                    }
                    totalMemory += entry.size;
                }
            }
        } finally {
            poolLock.unlock();
        }
        
        return new PoolStats(
            totalBuffers,
            inUseBuffers,
            totalAllocations.get(),
            totalReuses.get(),
            totalReleases.get(),
            totalEvictions.get(),
            totalCleanups.get(),
            totalMemory,
            underPressure
        );
    }
    
    


    public void resetStats() {
        totalAllocations.set(0);
        totalReuses.set(0);
        totalReleases.set(0);
        totalEvictions.set(0);
        totalCleanups.set(0);
    }
    
    


    public void clear() {
        poolLock.lock();
        try {
            bufferPools.clear();
            bufferById.clear();
            underPressure = false;
        } finally {
            poolLock.unlock();
        }
    }
    
    


    public static class PoolStats {
        public final int totalBuffers;
        public final int inUseBuffers;
        public final int totalAllocations;
        public final int totalReuses;
        public final int totalReleases;
        public final int totalEvictions;
        public final int totalCleanups;
        public final long totalMemory;
        public final boolean underPressure;
        
        public PoolStats(int totalBuffers, int inUseBuffers, int totalAllocations,
                        int totalReuses, int totalReleases, int totalEvictions,
                        int totalCleanups, long totalMemory, boolean underPressure) {
            this.totalBuffers = totalBuffers;
            this.inUseBuffers = inUseBuffers;
            this.totalAllocations = totalAllocations;
            this.totalReuses = totalReuses;
            this.totalReleases = totalReleases;
            this.totalEvictions = totalEvictions;
            this.totalCleanups = totalCleanups;
            this.totalMemory = totalMemory;
            this.underPressure = underPressure;
        }
    }
}
