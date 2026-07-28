package com.aetherflow;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;






public class ProtocolNegotiator {
    
    
    public static class ProtocolVersion {
        public final int major;
        public final int minor;
        public final int patch;
        public final String qualifier;
        
        public ProtocolVersion(int major, int minor, int patch, String qualifier) {
            this.major = major;
            this.minor = minor;
            this.patch = patch;
            this.qualifier = qualifier;
        }
        
        public ProtocolVersion(int major, int minor, int patch) {
            this(major, minor, patch, null);
        }
        
        public String getVersionString() {
            if (qualifier != null) {
                return major + "." + minor + "." + patch + "-" + qualifier;
            }
            return major + "." + minor + "." + patch;
        }
        
        public int compareTo(ProtocolVersion other) {
            if (this.major != other.major) {
                return Integer.compare(this.major, other.major);
            }
            if (this.minor != other.minor) {
                return Integer.compare(this.minor, other.minor);
            }
            if (this.patch != other.patch) {
                return Integer.compare(this.patch, other.patch);
            }
            return 0;
        }
        
        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;
            ProtocolVersion that = (ProtocolVersion) obj;
            return major == that.major && minor == that.minor && patch == that.patch;
        }
        
        @Override
        public int hashCode() {
            return Objects.hash(major, minor, patch);
        }
    }
    
    
    public static class ProtocolFeature {
        public final String name;
        public final String description;
        public final boolean required;
        public final Map<String, Object> parameters;
        
        public ProtocolFeature(String name, String description, boolean required, 
                             Map<String, Object> parameters) {
            this.name = name;
            this.description = description;
            this.required = required;
            this.parameters = parameters != null ? new HashMap<>(parameters) : new HashMap<>();
        }
        
        public ProtocolFeature(String name, String description, boolean required) {
            this(name, description, required, null);
        }
    }
    
    
    public static class NegotiationResult {
        public final ProtocolVersion negotiatedVersion;
        public final Set<ProtocolFeature> negotiatedFeatures;
        public final Set<ProtocolFeature> unsupportedFeatures;
        public final boolean success;
        public final String errorMessage;
        
        public NegotiationResult(ProtocolVersion negotiatedVersion, 
                                Set<ProtocolFeature> negotiatedFeatures,
                                Set<ProtocolFeature> unsupportedFeatures,
                                boolean success, String errorMessage) {
            this.negotiatedVersion = negotiatedVersion;
            this.negotiatedFeatures = negotiatedFeatures != null ? 
                new HashSet<>(negotiatedFeatures) : new HashSet<>();
            this.unsupportedFeatures = unsupportedFeatures != null ? 
                new HashSet<>(unsupportedFeatures) : new HashSet<>();
            this.success = success;
            this.errorMessage = errorMessage;
        }
        
        public static NegotiationResult success(ProtocolVersion version, 
                                               Set<ProtocolFeature> features) {
            return new NegotiationResult(version, features, null, true, null);
        }
        
        public static NegotiationResult failure(String errorMessage) {
            return new NegotiationResult(null, null, null, false, errorMessage);
        }
    }
    
    
    public enum NegotiationState {
        IDLE,
        PROPOSED,
        NEGOTIATING,
        COMPLETED,
        FAILED
    }
    
    
    public static class NegotiationSession {
        public final String sessionId;
        public final long creationTime;
        public volatile NegotiationState state;
        public volatile ProtocolVersion proposedVersion;
        public volatile ProtocolVersion acceptedVersion;
        public final Set<ProtocolFeature> proposedFeatures;
        public final Set<ProtocolFeature> acceptedFeatures;
        public final Set<ProtocolFeature> rejectedFeatures;
        public final Map<String, Object> sessionData;
        public final ReentrantLock sessionLock;
        
        public NegotiationSession(String sessionId) {
            this.sessionId = sessionId;
            this.creationTime = System.currentTimeMillis();
            this.state = NegotiationState.IDLE;
            this.proposedFeatures = new HashSet<>();
            this.acceptedFeatures = new HashSet<>();
            this.rejectedFeatures = new HashSet<>();
            this.sessionData = new ConcurrentHashMap<>();
            this.sessionLock = new ReentrantLock();
        }
        
        public long getAge() {
            return System.currentTimeMillis() - creationTime;
        }
    }
    
    
    public static class NegotiatorConfig {
        public ProtocolVersion minimumVersion;
        public ProtocolVersion maximumVersion;
        public ProtocolVersion preferredVersion;
        public Set<ProtocolFeature> supportedFeatures;
        public Set<ProtocolFeature> requiredFeatures;
        public long negotiationTimeout;
        public int maxRetries;
        public boolean allowDowngrade;
        public boolean strictMode;
        
        public NegotiatorConfig() {
            this.minimumVersion = new ProtocolVersion(2, 0, 0);
            this.maximumVersion = new ProtocolVersion(2, 1, 0);
            this.preferredVersion = new ProtocolVersion(2, 1, 0);
            this.supportedFeatures = new HashSet<>();
            this.requiredFeatures = new HashSet<>();
            this.negotiationTimeout = 30000;
            this.maxRetries = 3;
            this.allowDowngrade = true;
            this.strictMode = false;
        }
    }
    
    
    public static class NegotiationStats {
        public final AtomicInteger totalNegotiations;
        public final AtomicInteger successfulNegotiations;
        public final AtomicInteger failedNegotiations;
        public final AtomicInteger versionDowngrades;
        public final AtomicInteger featureNegotiations;
        public final AtomicInteger timeoutErrors;
        public final AtomicInteger protocolErrors;
        
        public NegotiationStats() {
            this.totalNegotiations = new AtomicInteger(0);
            this.successfulNegotiations = new AtomicInteger(0);
            this.failedNegotiations = new AtomicInteger(0);
            this.versionDowngrades = new AtomicInteger(0);
            this.featureNegotiations = new AtomicInteger(0);
            this.timeoutErrors = new AtomicInteger(0);
            this.protocolErrors = new AtomicInteger(0);
        }
    }
    
    
    private final NegotiatorConfig config;
    
    
    private final Map<String, NegotiationSession> sessions;
    
    
    private final AtomicLong sessionIdGenerator;
    
    
    private final NegotiationStats stats;
    
    
    private final ReentrantLock sessionLock;
    
    
    private final ScheduledExecutorService cleanupExecutor;
    
    
    private volatile boolean shutdown;
    
    


    public ProtocolNegotiator(NegotiatorConfig config) {
        this.config = config;
        this.sessions = new ConcurrentHashMap<>();
        this.sessionIdGenerator = new AtomicLong(0);
        this.stats = new NegotiationStats();
        this.sessionLock = new ReentrantLock();
        this.cleanupExecutor = Executors.newSingleThreadScheduledExecutor();
        this.shutdown = false;
        
        
        startCleanupThread();
    }
    
    


    public ProtocolNegotiator() {
        this(new NegotiatorConfig());
    }
    
    


    private void startCleanupThread() {
        cleanupExecutor.scheduleAtFixedRate(() -> {
            cleanupExpiredSessions();
        }, 60000, 60000, TimeUnit.MILLISECONDS);
    }
    
    


    public NegotiationSession createSession() {
        String sessionId = "neg_" + sessionIdGenerator.incrementAndGet() + "_" + 
                          System.currentTimeMillis();
        
        NegotiationSession session = new NegotiationSession(sessionId);
        sessions.put(sessionId, session);
        
        return session;
    }
    
    


    public void proposeVersion(NegotiationSession session, ProtocolVersion version) {
        session.sessionLock.lock();
        try {
            session.proposedVersion = version;
            session.state = NegotiationState.PROPOSED;
        } finally {
            session.sessionLock.unlock();
        }
    }
    
    


    public void proposeFeatures(NegotiationSession session, Set<ProtocolFeature> features) {
        session.sessionLock.lock();
        try {
            session.proposedFeatures.clear();
            session.proposedFeatures.addAll(features);
            session.state = NegotiationState.PROPOSED;
        } finally {
            session.sessionLock.unlock();
        }
    }
    
    


    public NegotiationResult negotiateVersion(NegotiationSession session, 
                                              ProtocolVersion remoteVersion) {
        session.sessionLock.lock();
        try {
            session.state = NegotiationState.NEGOTIATING;
            stats.totalNegotiations.incrementAndGet();
            
            
            if (remoteVersion.compareTo(config.minimumVersion) < 0) {
                return NegotiationResult.failure("Remote version " + 
                    remoteVersion.getVersionString() + " is below minimum " + 
                    config.minimumVersion.getVersionString());
            }
            
            if (remoteVersion.compareTo(config.maximumVersion) > 0) {
                if (!config.allowDowngrade) {
                    return NegotiationResult.failure("Remote version " + 
                        remoteVersion.getVersionString() + " is above maximum " + 
                        config.maximumVersion.getVersionString() + " and downgrade not allowed");
                }
                
                session.acceptedVersion = config.maximumVersion;
                stats.versionDowngrades.incrementAndGet();
            } else {
                
                session.acceptedVersion = remoteVersion;
            }
            
            session.state = NegotiationState.COMPLETED;
            stats.successfulNegotiations.incrementAndGet();
            
            return NegotiationResult.success(session.acceptedVersion, 
                                           session.acceptedFeatures);
            
        } finally {
            session.sessionLock.unlock();
        }
    }
    
    


    public NegotiationResult negotiateFeatures(NegotiationSession session, 
                                               Set<ProtocolFeature> remoteFeatures) {
        session.sessionLock.lock();
        try {
            session.state = NegotiationState.NEGOTIATING;
            stats.featureNegotiations.incrementAndGet();
            
            Set<ProtocolFeature> negotiated = new HashSet<>();
            Set<ProtocolFeature> unsupported = new HashSet<>();
            
            
            for (ProtocolFeature required : config.requiredFeatures) {
                if (!remoteFeatures.contains(required)) {
                    return NegotiationResult.failure("Required feature not supported: " + 
                        required.name);
                }
                negotiated.add(required);
            }
            
            
            for (ProtocolFeature remoteFeature : remoteFeatures) {
                if (config.supportedFeatures.contains(remoteFeature)) {
                    negotiated.add(remoteFeature);
                } else {
                    unsupported.add(remoteFeature);
                }
            }
            
            session.acceptedFeatures.clear();
            session.acceptedFeatures.addAll(negotiated);
            session.rejectedFeatures.clear();
            session.rejectedFeatures.addAll(unsupported);
            
            session.state = NegotiationState.COMPLETED;
            stats.successfulNegotiations.incrementAndGet();
            
            return NegotiationResult.success(session.acceptedVersion, negotiated);
            
        } finally {
            session.sessionLock.unlock();
        }
    }
    
    


    public NegotiationResult negotiate(NegotiationSession session, 
                                      ProtocolVersion remoteVersion,
                                      Set<ProtocolFeature> remoteFeatures) {
        session.sessionLock.lock();
        try {
            
            NegotiationResult versionResult = negotiateVersion(session, remoteVersion);
            if (!versionResult.success) {
                session.state = NegotiationState.FAILED;
                stats.failedNegotiations.incrementAndGet();
                return versionResult;
            }
            
            
            NegotiationResult featureResult = negotiateFeatures(session, remoteFeatures);
            if (!featureResult.success) {
                session.state = NegotiationState.FAILED;
                stats.failedNegotiations.incrementAndGet();
                return featureResult;
            }
            
            session.state = NegotiationState.COMPLETED;
            return NegotiationResult.success(session.acceptedVersion, 
                                           session.acceptedFeatures);
            
        } finally {
            session.sessionLock.unlock();
        }
    }
    
    


    public void acceptNegotiation(NegotiationSession session) {
        session.sessionLock.lock();
        try {
            session.state = NegotiationState.COMPLETED;
        } finally {
            session.sessionLock.unlock();
        }
    }
    
    


    public void rejectNegotiation(NegotiationSession session, String reason) {
        session.sessionLock.lock();
        try {
            session.state = NegotiationState.FAILED;
            session.sessionData.put("rejectionReason", reason);
            stats.failedNegotiations.incrementAndGet();
        } finally {
            session.sessionLock.unlock();
        }
    }
    
    


    public NegotiationSession getSession(String sessionId) {
        return sessions.get(sessionId);
    }
    
    


    public Collection<NegotiationSession> getSessions() {
        return new ArrayList<>(sessions.values());
    }
    
    


    private void cleanupExpiredSessions() {
        long now = System.currentTimeMillis();
        
        sessionLock.lock();
        try {
            Iterator<Map.Entry<String, NegotiationSession>> it = sessions.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<String, NegotiationSession> entry = it.next();
                NegotiationSession session = entry.getValue();
                
                if (session.getAge() > config.negotiationTimeout) {
                    it.remove();
                    if (session.state == NegotiationState.NEGOTIATING) {
                        stats.timeoutErrors.incrementAndGet();
                    }
                }
            }
        } finally {
            sessionLock.unlock();
        }
    }
    
    


    public NegotiationStats getStats() {
        return stats;
    }
    
    


    public NegotiatorConfig getConfig() {
        return config;
    }
    
    


    public void addSupportedFeature(ProtocolFeature feature) {
        config.supportedFeatures.add(feature);
    }
    
    


    public void addRequiredFeature(ProtocolFeature feature) {
        config.requiredFeatures.add(feature);
        config.supportedFeatures.add(feature);
    }
    
    


    public void removeSupportedFeature(ProtocolFeature feature) {
        config.supportedFeatures.remove(feature);
        config.requiredFeatures.remove(feature);
    }
    
    


    public void shutdown() {
        shutdown = true;
        cleanupExecutor.shutdown();
        try {
            cleanupExecutor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        sessions.clear();
    }
}
