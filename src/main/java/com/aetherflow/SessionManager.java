package com.aetherflow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;





public class SessionManager {
    
    
    public static class SessionEntry {
        public final String sessionId;
        public final String userId;
        public final String authToken;
        public volatile long creationTime;
        public volatile long lastAccessTime;
        public volatile long expiryTime;
        public volatile boolean active;
        public volatile boolean authenticated;
        public volatile String role;
        public final Map<String, String> attributes;
        public final List<String> permissions;
        public volatile int accessCount;
        public volatile ConnectionStateMachine.ConnectionState associatedConnectionState;
        public volatile int associatedConnectionId;
        
        public SessionEntry(String sessionId, String userId, String authToken, long ttl) {
            this.sessionId = sessionId;
            this.userId = userId;
            this.authToken = authToken;
            this.creationTime = System.currentTimeMillis();
            this.lastAccessTime = System.currentTimeMillis();
            this.expiryTime = creationTime + ttl;
            this.active = true;
            this.authenticated = false;
            this.role = "guest";
            this.attributes = new HashMap<>();
            this.permissions = new ArrayList<>();
            this.accessCount = 0;
            this.associatedConnectionState = null;
            this.associatedConnectionId = 0;
        }
        
        public String getSessionId() {
            return sessionId;
        }
        
        public String getUserId() {
            return userId;
        }
        
        public void recordAccess() {
            accessCount++;
            lastAccessTime = System.currentTimeMillis();
        }
        
        public boolean isExpired() {
            return System.currentTimeMillis() > expiryTime;
        }
        
        public long getTimeToExpiry() {
            return Math.max(0, expiryTime - System.currentTimeMillis());
        }
        
        public long getAge() {
            return System.currentTimeMillis() - creationTime;
        }
        
        public long getIdleTime() {
            return System.currentTimeMillis() - lastAccessTime;
        }
        
        public void authenticate(String role) {
            this.authenticated = true;
            this.role = role;
        }
        
        public void addPermission(String permission) {
            if (!permissions.contains(permission)) {
                permissions.add(permission);
            }
        }
        
        public boolean hasPermission(String permission) {
            return permissions.contains(permission);
        }
        
        public void setAttribute(String key, String value) {
            attributes.put(key, value);
        }
        
        public String getAttribute(String key) {
            return attributes.get(key);
        }
    }
    
    
    public static class AuthChallenge {
        public final String challengeId;
        public final String sessionId;
        public final byte[] challengeData;
        public final long creationTime;
        public volatile boolean answered;
        public volatile String answer;
        
        public AuthChallenge(String challengeId, String sessionId, byte[] challengeData) {
            this.challengeId = challengeId;
            this.sessionId = sessionId;
            this.challengeData = challengeData != null ? challengeData.clone() : new byte[0];
            this.creationTime = System.currentTimeMillis();
            this.answered = false;
            this.answer = null;
        }
        
        public boolean isExpired(long ttl) {
            return System.currentTimeMillis() - creationTime > ttl;
        }
    }
    
    
    private static final int MAX_SESSIONS = 10000;
    private static final long DEFAULT_SESSION_TTL_MS = 3600000; 
    private static final long AUTH_CHALLENGE_TTL_MS = 300000; 
    private static final long SESSION_IDLE_TIMEOUT_MS = 1800000; 
    private static final int MAX_PERMISSIONS_PER_SESSION = 100;
    
    
    private final Map<String, SessionEntry> sessions;
    
    
    private final Map<String, List<String>> sessionsByUser;
    
    
    private final Map<String, String> sessionByAuthToken;
    
    
    private final Map<String, AuthChallenge> authChallenges;
    
    
    private final ReentrantReadWriteLock sessionLock;
    private final ReentrantLock authLock;
    
    
    private final AtomicInteger totalSessionsCreated;
    private final AtomicInteger totalSessionsExpired;
    private final AtomicInteger totalSessionsRevoked;
    private final AtomicInteger totalAuthAttempts;
    private final AtomicInteger totalAuthSuccesses;
    private final AtomicInteger totalAuthFailures;
    
    
    private volatile long sessionTtl;
    private volatile long sessionIdleTimeout;
    
    
    private final AtomicLong sessionIdGenerator;
    private final AtomicLong challengeIdGenerator;
    
    
    private volatile Thread cleanupThread;
    private volatile boolean cleanupThreadRunning;
    
    


    public SessionManager() {
        this.sessions = new ConcurrentHashMap<>();
        this.sessionsByUser = new ConcurrentHashMap<>();
        this.sessionByAuthToken = new ConcurrentHashMap<>();
        this.authChallenges = new ConcurrentHashMap<>();
        this.sessionLock = new ReentrantReadWriteLock();
        this.authLock = new ReentrantLock();
        this.totalSessionsCreated = new AtomicInteger(0);
        this.totalSessionsExpired = new AtomicInteger(0);
        this.totalSessionsRevoked = new AtomicInteger(0);
        this.totalAuthAttempts = new AtomicInteger(0);
        this.totalAuthSuccesses = new AtomicInteger(0);
        this.totalAuthFailures = new AtomicInteger(0);
        this.sessionTtl = DEFAULT_SESSION_TTL_MS;
        this.sessionIdleTimeout = SESSION_IDLE_TIMEOUT_MS;
        this.sessionIdGenerator = new AtomicLong(0);
        this.challengeIdGenerator = new AtomicLong(0);
        
        startCleanupThread();
    }
    
    


    public SessionEntry createSession(String userId, String authToken) {
        if (userId == null || authToken == null) {
            return null;
        }
        
        sessionLock.writeLock().lock();
        try {
            
            if (sessions.size() >= MAX_SESSIONS) {
                
                cleanupExpiredSessions(100);
                
                if (sessions.size() >= MAX_SESSIONS) {
                    return null;
                }
            }
            
            
            String sessionId = generateSessionId();
            
            
            SessionEntry session = new SessionEntry(sessionId, userId, authToken, sessionTtl);
            
            
            sessions.put(sessionId, session);
            
            
            List<String> userSessions = sessionsByUser.computeIfAbsent(userId, k -> new ArrayList<>());
            userSessions.add(sessionId);
            
            
            sessionByAuthToken.put(authToken, sessionId);
            
            totalSessionsCreated.incrementAndGet();
            
            return session;
            
        } finally {
            sessionLock.writeLock().unlock();
        }
    }
    
    


    public SessionEntry getSession(String sessionId) {
        sessionLock.readLock().lock();
        try {
            SessionEntry session = sessions.get(sessionId);
            if (session != null) {
                session.recordAccess();
            }
            return session;
        } finally {
            sessionLock.readLock().unlock();
        }
    }
    
    


    public SessionEntry getSessionByAuthToken(String authToken) {
        sessionLock.readLock().lock();
        try {
            String sessionId = sessionByAuthToken.get(authToken);
            if (sessionId == null) {
                return null;
            }
            return getSession(sessionId);
        } finally {
            sessionLock.readLock().unlock();
        }
    }
    
    


    public boolean validateSession(String sessionId) {
        SessionEntry session = getSession(sessionId);
        if (session == null) {
            return false;
        }
        
        return session.active && !session.isExpired();
    }
    
    


    public boolean authenticateSession(String sessionId, String role) {
        SessionEntry session = getSession(sessionId);
        if (session == null) {
            totalAuthFailures.incrementAndGet();
            return false;
        }
        
        totalAuthAttempts.incrementAndGet();
        
        sessionLock.writeLock().lock();
        try {
            if (!session.active || session.isExpired()) {
                totalAuthFailures.incrementAndGet();
                return false;
            }
            
            
            
            session.authenticate(role);
            
            
            
            if (session.role == null) {
                session.role = role;
            }
            
            totalAuthSuccesses.incrementAndGet();
            return true;
            
        } finally {
            sessionLock.writeLock().unlock();
        }
    }
    
    


    public AuthChallenge createAuthChallenge(String sessionId) {
        SessionEntry session = getSession(sessionId);
        if (session == null) {
            return null;
        }
        
        authLock.lock();
        try {
            String challengeId = generateChallengeId();
            byte[] challengeData = generateChallengeData();
            
            AuthChallenge challenge = new AuthChallenge(challengeId, sessionId, challengeData);
            authChallenges.put(challengeId, challenge);
            
            return challenge;
            
        } finally {
            authLock.unlock();
        }
    }
    
    


    public boolean answerAuthChallenge(String challengeId, String answer) {
        authLock.lock();
        try {
            AuthChallenge challenge = authChallenges.get(challengeId);
            if (challenge == null) {
                return false;
            }
            
            if (challenge.isExpired(AUTH_CHALLENGE_TTL_MS)) {
                authChallenges.remove(challengeId);
                return false;
            }
            
            challenge.answered = true;
            challenge.answer = answer;
            
            
            boolean valid = validateChallengeAnswer(challenge, answer);
            
            if (valid) {
                SessionEntry session = getSession(challenge.sessionId);
                if (session != null) {
                    session.authenticate("authenticated");
                }
            }
            
            authChallenges.remove(challengeId);
            
            return valid;
            
        } finally {
            authLock.unlock();
        }
    }
    
    


    public boolean grantPermission(String sessionId, String permission) {
        SessionEntry session = getSession(sessionId);
        if (session == null || !session.authenticated) {
            return false;
        }
        
        sessionLock.writeLock().lock();
        try {
            if (session.permissions.size() >= MAX_PERMISSIONS_PER_SESSION) {
                return false;
            }
            
            session.addPermission(permission);
            return true;
            
        } finally {
            sessionLock.writeLock().unlock();
        }
    }
    
    


    public boolean revokeSession(String sessionId) {
        sessionLock.writeLock().lock();
        try {
            SessionEntry session = sessions.remove(sessionId);
            if (session == null) {
                return false;
            }
            
            session.active = false;
            
            
            List<String> userSessions = sessionsByUser.get(session.userId);
            if (userSessions != null) {
                userSessions.remove(sessionId);
                if (userSessions.isEmpty()) {
                    sessionsByUser.remove(session.userId);
                }
            }
            
            
            sessionByAuthToken.remove(session.authToken);
            
            totalSessionsRevoked.incrementAndGet();
            
            return true;
            
        } finally {
            sessionLock.writeLock().unlock();
        }
    }
    
    


    public boolean refreshSession(String sessionId) {
        SessionEntry session = getSession(sessionId);
        if (session == null) {
            return false;
        }
        
        sessionLock.writeLock().lock();
        try {
            if (!session.active || session.isExpired()) {
                return false;
            }
            
            session.expiryTime = System.currentTimeMillis() + sessionTtl;
            session.lastAccessTime = System.currentTimeMillis();
            
            return true;
            
        } finally {
            sessionLock.writeLock().unlock();
        }
    }
    
    


    public List<SessionEntry> getUserSessions(String userId) {
        sessionLock.readLock().lock();
        try {
            List<String> sessionIds = sessionsByUser.get(userId);
            if (sessionIds == null) {
                return new ArrayList<>();
            }
            
            List<SessionEntry> userSessions = new ArrayList<>();
            for (String sessionId : sessionIds) {
                SessionEntry session = sessions.get(sessionId);
                if (session != null) {
                    userSessions.add(session);
                }
            }
            
            return userSessions;
            
        } finally {
            sessionLock.readLock().unlock();
        }
    }
    
    


    public int cleanupExpiredSessions(int maxToClean) {
        int cleaned = 0;
        
        sessionLock.writeLock().lock();
        try {
            List<String> toRemove = new ArrayList<>();
            
            for (Map.Entry<String, SessionEntry> entry : sessions.entrySet()) {
                SessionEntry session = entry.getValue();
                if (!session.active || session.isExpired() || session.getIdleTime() > sessionIdleTimeout) {
                    toRemove.add(entry.getKey());
                    if (toRemove.size() >= maxToClean) {
                        break;
                    }
                }
            }
            
            for (String sessionId : toRemove) {
                SessionEntry session = sessions.remove(sessionId);
                if (session != null) {
                    
                    List<String> userSessions = sessionsByUser.get(session.userId);
                    if (userSessions != null) {
                        userSessions.remove(sessionId);
                        if (userSessions.isEmpty()) {
                            sessionsByUser.remove(session.userId);
                        }
                    }
                    
                    
                    sessionByAuthToken.remove(session.authToken);
                    
                    cleaned++;
                    totalSessionsExpired.incrementAndGet();
                }
            }
            
        } finally {
            sessionLock.writeLock().unlock();
        }
        
        return cleaned;
    }
    
    


    public SessionStats getStats() {
        sessionLock.readLock().lock();
        try {
            int totalSessions = sessions.size();
            int activeSessions = 0;
            int authenticatedSessions = 0;
            int expiredSessions = 0;
            
            for (SessionEntry session : sessions.values()) {
                if (session.active && !session.isExpired()) {
                    activeSessions++;
                }
                if (session.authenticated) {
                    authenticatedSessions++;
                }
                if (session.isExpired()) {
                    expiredSessions++;
                }
            }
            
            return new SessionStats(
                totalSessions,
                activeSessions,
                authenticatedSessions,
                expiredSessions,
                totalSessionsCreated.get(),
                totalSessionsExpired.get(),
                totalSessionsRevoked.get(),
                totalAuthAttempts.get(),
                totalAuthSuccesses.get(),
                totalAuthFailures.get()
            );
            
        } finally {
            sessionLock.readLock().unlock();
        }
    }
    
    


    private String generateSessionId() {
        
        
        long id = sessionIdGenerator.incrementAndGet();
        
        
        
        if (id == Long.MAX_VALUE) {
            
            
            sessionIdGenerator.set(0);
        }
        
        return "sess_" + System.currentTimeMillis() + "_" + id;
    }
    
    


    private String generateChallengeId() {
        return "chal_" + System.currentTimeMillis() + "_" + challengeIdGenerator.incrementAndGet();
    }
    
    


    private byte[] generateChallengeData() {
        byte[] data = new byte[32];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (Math.random() * 256);
        }
        return data;
    }
    
    


    private boolean validateChallengeAnswer(AuthChallenge challenge, String answer) {
        
        
        return answer != null && !answer.isEmpty();
    }
    
    


    private void startCleanupThread() {
        if (cleanupThread != null && cleanupThread.isAlive()) {
            return;
        }
        
        cleanupThreadRunning = true;
        cleanupThread = new Thread(() -> {
            while (cleanupThreadRunning) {
                try {
                    Thread.sleep(60000); 
                    
                    
                    cleanupExpiredSessions(100);
                    
                    
                    cleanupExpiredChallenges();
                    
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }, "SessionManager-Cleanup");
        
        cleanupThread.setDaemon(true);
        cleanupThread.start();
    }
    
    


    private void cleanupExpiredChallenges() {
        authLock.lock();
        try {
            List<String> toRemove = new ArrayList<>();
            
            for (Map.Entry<String, AuthChallenge> entry : authChallenges.entrySet()) {
                if (entry.getValue().isExpired(AUTH_CHALLENGE_TTL_MS)) {
                    toRemove.add(entry.getKey());
                }
            }
            
            for (String challengeId : toRemove) {
                authChallenges.remove(challengeId);
            }
            
        } finally {
            authLock.unlock();
        }
    }
    
    


    public void stopCleanupThread() {
        cleanupThreadRunning = false;
        if (cleanupThread != null) {
            cleanupThread.interrupt();
        }
    }
    
    


    public void setSessionTtl(long ttlMs) {
        this.sessionTtl = ttlMs;
    }
    
    


    public void setSessionIdleTimeout(long timeoutMs) {
        this.sessionIdleTimeout = timeoutMs;
    }
    
    


    public void clear() {
        sessionLock.writeLock().lock();
        try {
            sessions.clear();
            sessionsByUser.clear();
            sessionByAuthToken.clear();
        } finally {
            sessionLock.writeLock().unlock();
        }
        
        authLock.lock();
        try {
            authChallenges.clear();
        } finally {
            authLock.unlock();
        }
    }
    
    


    public static class SessionStats {
        public final int totalSessions;
        public final int activeSessions;
        public final int authenticatedSessions;
        public final int expiredSessions;
        public final int totalSessionsCreated;
        public final int totalSessionsExpired;
        public final int totalSessionsRevoked;
        public final int totalAuthAttempts;
        public final int totalAuthSuccesses;
        public final int totalAuthFailures;
        
        public SessionStats(int totalSessions, int activeSessions, int authenticatedSessions,
                          int expiredSessions, int totalSessionsCreated, int totalSessionsExpired,
                          int totalSessionsRevoked, int totalAuthAttempts, int totalAuthSuccesses,
                          int totalAuthFailures) {
            this.totalSessions = totalSessions;
            this.activeSessions = activeSessions;
            this.authenticatedSessions = authenticatedSessions;
            this.expiredSessions = expiredSessions;
            this.totalSessionsCreated = totalSessionsCreated;
            this.totalSessionsExpired = totalSessionsExpired;
            this.totalSessionsRevoked = totalSessionsRevoked;
            this.totalAuthAttempts = totalAuthAttempts;
            this.totalAuthSuccesses = totalAuthSuccesses;
            this.totalAuthFailures = totalAuthFailures;
        }
    }
}
