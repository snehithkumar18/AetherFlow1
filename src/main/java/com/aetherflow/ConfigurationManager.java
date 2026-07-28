package com.aetherflow;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;
import java.io.*;
import java.nio.file.*;
import java.util.function.*;






public class ConfigurationManager {
    
    
    public enum ConfigSourceType {
        FILE,
        ENVIRONMENT,
        SYSTEM_PROPERTIES,
        PROGRAMMATIC,
        REMOTE,
        DATABASE
    }
    
    
    public static class ConfigEntry {
        public final String key;
        public final String value;
        public final ConfigSourceType source;
        public final long lastModified;
        public final String sourceLocation;
        public volatile boolean encrypted;
        public final Map<String, String> metadata;
        
        public ConfigEntry(String key, String value, ConfigSourceType source, 
                          String sourceLocation, Map<String, String> metadata) {
            this.key = key;
            this.value = value;
            this.source = source;
            this.lastModified = System.currentTimeMillis();
            this.sourceLocation = sourceLocation;
            this.encrypted = false;
            this.metadata = metadata != null ? new HashMap<>(metadata) : new HashMap<>();
        }
        
        public ConfigEntry(String key, String value, ConfigSourceType source) {
            this(key, value, source, null, null);
        }
        
        public <T> T getValueAs(Class<T> type) {
            if (type == String.class) {
                return type.cast(value);
            } else if (type == Integer.class || type == int.class) {
                return type.cast(Integer.parseInt(value));
            } else if (type == Long.class || type == long.class) {
                return type.cast(Long.parseLong(value));
            } else if (type == Double.class || type == double.class) {
                return type.cast(Double.parseDouble(value));
            } else if (type == Boolean.class || type == boolean.class) {
                return type.cast(Boolean.parseBoolean(value));
            } else {
                throw new IllegalArgumentException("Unsupported type: " + type);
            }
        }
    }
    
    
    public interface ConfigValidator {
        boolean validate(String key, String value);
        String getErrorMessage();
    }
    
    
    public interface ConfigChangeListener {
        void onConfigChanged(String key, String oldValue, String newValue);
        void onConfigAdded(String key, String value);
        void onConfigRemoved(String key);
    }
    
    
    public static class ConfigManagerConfig {
        public boolean enableHotReload;
        public long reloadInterval;
        public boolean enableValidation;
        public boolean enableEncryption;
        public String encryptionKey;
        public boolean enableBackup;
        public int backupCount;
        public boolean enableRemoteSync;
        public String remoteSyncUrl;
        public long remoteSyncInterval;
        public boolean enableCache;
        public long cacheTimeout;
        
        public ConfigManagerConfig() {
            this.enableHotReload = true;
            this.reloadInterval = 60000; 
            this.enableValidation = true;
            this.enableEncryption = false;
            this.encryptionKey = null;
            this.enableBackup = true;
            this.backupCount = 5;
            this.enableRemoteSync = false;
            this.remoteSyncUrl = null;
            this.remoteSyncInterval = 300000; 
            this.enableCache = true;
            this.cacheTimeout = 300000; 
        }
    }
    
    
    public static class ConfigStats {
        public final AtomicLong totalReads;
        public final AtomicLong totalWrites;
        public final AtomicLong totalReloads;
        public final AtomicLong totalValidations;
        public final AtomicLong validationFailures;
        public final Map<String, AtomicLong> keyReadCounts;
        public final Map<String, AtomicLong> keyWriteCounts;
        public final Map<String, AtomicLong> sourceCounts;
        
        public ConfigStats() {
            this.totalReads = new AtomicLong(0);
            this.totalWrites = new AtomicLong(0);
            this.totalReloads = new AtomicLong(0);
            this.totalValidations = new AtomicLong(0);
            this.validationFailures = new AtomicLong(0);
            this.keyReadCounts = new ConcurrentHashMap<>();
            this.keyWriteCounts = new ConcurrentHashMap<>();
            this.sourceCounts = new ConcurrentHashMap<>();
        }
        
        public void recordRead(String key, ConfigSourceType source) {
            totalReads.incrementAndGet();
            keyReadCounts.computeIfAbsent(key, k -> new AtomicLong(0)).incrementAndGet();
            sourceCounts.computeIfAbsent(source.name(), k -> new AtomicLong(0)).incrementAndGet();
        }
        
        public void recordWrite(String key, ConfigSourceType source) {
            totalWrites.incrementAndGet();
            keyWriteCounts.computeIfAbsent(key, k -> new AtomicLong(0)).incrementAndGet();
            sourceCounts.computeIfAbsent(source.name(), k -> new AtomicLong(0)).incrementAndGet();
        }
        
        public void recordReload() {
            totalReloads.incrementAndGet();
        }
        
        public void recordValidation(boolean success) {
            totalValidations.incrementAndGet();
            if (!success) {
                validationFailures.incrementAndGet();
            }
        }
    }
    
    
    private final ConfigManagerConfig config;
    
    
    private final Map<String, ConfigEntry> configEntries;
    
    
    private final Map<String, ConfigValidator> validators;
    
    
    private final List<ConfigChangeListener> listeners;
    
    
    private final Map<String, CacheEntry> configCache;
    
    
    private final List<Map<String, ConfigEntry>> backups;
    
    
    private final ConfigStats stats;
    
    
    private final ReentrantReadWriteLock configLock;
    
    
    private final ScheduledExecutorService reloadExecutor;
    
    
    private volatile boolean shutdown;
    
    
    private static class CacheEntry {
        public final String value;
        public final long timestamp;
        
        public CacheEntry(String value) {
            this.value = value;
            this.timestamp = System.currentTimeMillis();
        }
        
        public boolean isExpired(long timeout) {
            return System.currentTimeMillis() - timestamp > timeout;
        }
    }
    
    


    public ConfigurationManager(ConfigManagerConfig config) {
        this.config = config;
        this.configEntries = new ConcurrentHashMap<>();
        this.validators = new ConcurrentHashMap<>();
        this.listeners = new CopyOnWriteArrayList<>();
        this.configCache = new ConcurrentHashMap<>();
        this.backups = new ArrayList<>();
        this.stats = new ConfigStats();
        this.configLock = new ReentrantReadWriteLock();
        this.reloadExecutor = Executors.newSingleThreadScheduledExecutor();
        this.shutdown = false;
        
        
        if (config.enableHotReload) {
            startHotReloadThread();
        }
    }
    
    


    public ConfigurationManager() {
        this(new ConfigManagerConfig());
    }
    
    


    private void startHotReloadThread() {
        reloadExecutor.scheduleAtFixedRate(() -> {
            reloadConfiguration();
        }, config.reloadInterval, config.reloadInterval, TimeUnit.MILLISECONDS);
    }
    
    


    public void setConfig(String key, String value) {
        setConfig(key, value, ConfigSourceType.PROGRAMMATIC, null);
    }
    
    


    public void setConfig(String key, String value, ConfigSourceType source, 
                         String sourceLocation) {
        configLock.writeLock().lock();
        try {
            
            if (config.enableValidation) {
                ConfigValidator validator = validators.get(key);
                if (validator != null) {
                    boolean valid = validator.validate(key, value);
                    stats.recordValidation(valid);
                    if (!valid) {
                        throw new IllegalArgumentException("Validation failed: " + 
                            validator.getErrorMessage());
                    }
                }
            }
            
            String oldValue = configEntries.containsKey(key) ? 
                configEntries.get(key).value : null;
            
            ConfigEntry entry = new ConfigEntry(key, value, source, sourceLocation, null);
            configEntries.put(key, entry);
            
            
            configCache.remove(key);
            
            
            if (config.enableBackup) {
                createBackup();
            }
            
            stats.recordWrite(key, source);
            
            
            for (ConfigChangeListener listener : listeners) {
                if (oldValue != null) {
                    listener.onConfigChanged(key, oldValue, value);
                } else {
                    listener.onConfigAdded(key, value);
                }
            }
            
        } finally {
            configLock.writeLock().unlock();
        }
    }
    
    


    public String getConfig(String key) {
        configLock.readLock().lock();
        try {
            
            if (config.enableCache) {
                CacheEntry cached = configCache.get(key);
                if (cached != null && !cached.isExpired(config.cacheTimeout)) {
                    return cached.value;
                }
            }
            
            ConfigEntry entry = configEntries.get(key);
            if (entry == null) {
                return null;
            }
            
            
            if (config.enableCache) {
                configCache.put(key, new CacheEntry(entry.value));
            }
            
            stats.recordRead(key, entry.source);
            
            return entry.value;
            
        } finally {
            configLock.readLock().unlock();
        }
    }
    
    


    public String getConfig(String key, String defaultValue) {
        String value = getConfig(key);
        return value != null ? value : defaultValue;
    }
    
    


    public <T> T getConfigAs(String key, Class<T> type) {
        String value = getConfig(key);
        if (value == null) {
            return null;
        }
        
        ConfigEntry entry = configEntries.get(key);
        if (entry == null) {
            return null;
        }
        
        return entry.getValueAs(type);
    }
    
    


    public <T> T getConfigAs(String key, Class<T> type, T defaultValue) {
        T value = getConfigAs(key, type);
        return value != null ? value : defaultValue;
    }
    
    


    public void removeConfig(String key) {
        configLock.writeLock().lock();
        try {
            ConfigEntry removed = configEntries.remove(key);
            if (removed != null) {
                configCache.remove(key);
                
                
                for (ConfigChangeListener listener : listeners) {
                    listener.onConfigRemoved(key);
                }
            }
        } finally {
            configLock.writeLock().unlock();
        }
    }
    
    


    public boolean hasConfig(String key) {
        configLock.readLock().lock();
        try {
            return configEntries.containsKey(key);
        } finally {
            configLock.readLock().unlock();
        }
    }
    
    


    public Set<String> getConfigKeys() {
        configLock.readLock().lock();
        try {
            return new HashSet<>(configEntries.keySet());
        } finally {
            configLock.readLock().unlock();
        }
    }
    
    


    public Map<String, ConfigEntry> getConfigEntries() {
        configLock.readLock().lock();
        try {
            return new HashMap<>(configEntries);
        } finally {
            configLock.readLock().unlock();
        }
    }
    
    


    public void addValidator(String key, ConfigValidator validator) {
        validators.put(key, validator);
    }
    
    


    public void removeValidator(String key) {
        validators.remove(key);
    }
    
    


    public void addChangeListener(ConfigChangeListener listener) {
        listeners.add(listener);
    }
    
    


    public void removeChangeListener(ConfigChangeListener listener) {
        listeners.remove(listener);
    }
    
    


    public void loadFromFile(String filePath) throws IOException {
        Path path = Paths.get(filePath);
        List<String> lines = Files.readAllLines(path);
        
        for (String line : lines) {
            line = line.trim();
            if (line.isEmpty() || line.startsWith("#")) {
                continue;
            }
            
            String[] parts = line.split("=", 2);
            if (parts.length == 2) {
                String key = parts[0].trim();
                String value = parts[1].trim();
                setConfig(key, value, ConfigSourceType.FILE, filePath);
            }
        }
    }
    
    


    public void loadFromEnvironment() {
        Map<String, String> env = System.getenv();
        for (Map.Entry<String, String> entry : env.entrySet()) {
            if (entry.getKey().startsWith("NEXUS_")) {
                String key = entry.getKey().substring(7).toLowerCase();
                setConfig(key, entry.getValue(), ConfigSourceType.ENVIRONMENT, "environment");
            }
        }
    }
    
    


    public void loadFromSystemProperties() {
        Properties props = System.getProperties();
        for (String key : props.stringPropertyNames()) {
            if (key.startsWith("nexus.")) {
                String configKey = key.substring(7);
                setConfig(configKey, props.getProperty(key), ConfigSourceType.SYSTEM_PROPERTIES, 
                        "system");
            }
        }
    }
    
    


    public void saveToFile(String filePath) throws IOException {
        configLock.readLock().lock();
        try {
            List<String> lines = new ArrayList<>();
            
            for (Map.Entry<String, ConfigEntry> entry : configEntries.entrySet()) {
                lines.add(entry.getKey() + "=" + entry.getValue().value);
            }
            
            Files.write(Paths.get(filePath), lines);
        } finally {
            configLock.readLock().unlock();
        }
    }
    
    


    private void createBackup() {
        Map<String, ConfigEntry> backup = new HashMap<>(configEntries);
        backups.add(backup);
        
        
        while (backups.size() > config.backupCount) {
            backups.remove(0);
        }
    }
    
    


    public void restoreFromBackup(int backupIndex) {
        if (backupIndex < 0 || backupIndex >= backups.size()) {
            throw new IllegalArgumentException("Invalid backup index: " + backupIndex);
        }
        
        configLock.writeLock().lock();
        try {
            Map<String, ConfigEntry> backup = backups.get(backupIndex);
            configEntries.clear();
            configEntries.putAll(backup);
            configCache.clear();
        } finally {
            configLock.writeLock().unlock();
        }
    }
    
    


    public void reloadConfiguration() {
        stats.recordReload();
        
        
        
        configCache.clear();
    }
    
    


    public void clearCache() {
        configCache.clear();
    }
    
    


    public ConfigStats getStats() {
        return stats;
    }
    
    


    public ConfigManagerConfig getConfig() {
        return config;
    }
    
    


    public int getBackupCount() {
        return backups.size();
    }
    
    


    public void shutdown() {
        shutdown = true;
        
        reloadExecutor.shutdown();
        try {
            reloadExecutor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        configEntries.clear();
        configCache.clear();
        backups.clear();
        validators.clear();
        listeners.clear();
    }
}
