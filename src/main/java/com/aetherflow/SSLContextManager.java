package com.aetherflow;

import javax.net.ssl.*;
import java.io.*;
import java.security.*;
import java.security.cert.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;






public class SSLContextManager {
    
    
    public enum SSLProtocol {
        TLSv1_2("TLSv1.2"),
        TLSv1_3("TLSv1.3");
        
        private final String protocolName;
        
        SSLProtocol(String protocolName) {
            this.protocolName = protocolName;
        }
        
        public String getProtocolName() {
            return protocolName;
        }
    }
    
    
    public static class SSLConfig {
        public SSLProtocol protocol;
        public String[] enabledCipherSuites;
        public String[] enabledProtocols;
        public boolean clientAuth;
        public boolean hostnameVerification;
        public int sessionTimeout;
        public int sessionCacheSize;
        public boolean enableSessionResumption;
        public String trustStorePath;
        public String trustStorePassword;
        public String keyStorePath;
        public String keyStorePassword;
        public String keyPassword;
        
        public SSLConfig() {
            this.protocol = SSLProtocol.TLSv1_3;
            this.enabledCipherSuites = null; 
            this.enabledProtocols = null; 
            this.clientAuth = false;
            this.hostnameVerification = true;
            this.sessionTimeout = 300; 
            this.sessionCacheSize = 1000;
            this.enableSessionResumption = true;
            this.trustStorePath = null;
            this.trustStorePassword = null;
            this.keyStorePath = null;
            this.keyStorePassword = null;
            this.keyPassword = null;
        }
    }
    
    
    public static class SSLSessionInfo {
        public final String sessionId;
        public final long creationTime;
        public final long lastAccessedTime;
        public final long timeout;
        public final String protocol;
        public final String cipherSuite;
        public final String peerHost;
        public final int peerPort;
        public volatile boolean valid;
        
        public SSLSessionInfo(String sessionId, SSLSession session, String peerHost, int peerPort) {
            this.sessionId = sessionId;
            this.creationTime = session.getCreationTime();
            this.lastAccessedTime = session.getLastAccessedTime();
            this.timeout = 300; 
            this.protocol = session.getProtocol();
            this.cipherSuite = session.getCipherSuite();
            this.peerHost = peerHost;
            this.peerPort = peerPort;
            this.valid = true;
        }
        
        public boolean isExpired() {
            return System.currentTimeMillis() - lastAccessedTime > timeout * 1000;
        }
    }
    
    
    public static class SSLStats {
        public final AtomicLong totalHandshakes;
        public final AtomicLong successfulHandshakes;
        public final AtomicLong failedHandshakes;
        public final AtomicLong sessionResumptions;
        public final AtomicLong sessionCreations;
        public final AtomicLong certificateErrors;
        public final AtomicLong verificationErrors;
        public final AtomicLong activeSessions;
        
        public SSLStats() {
            this.totalHandshakes = new AtomicLong(0);
            this.successfulHandshakes = new AtomicLong(0);
            this.failedHandshakes = new AtomicLong(0);
            this.sessionResumptions = new AtomicLong(0);
            this.sessionCreations = new AtomicLong(0);
            this.certificateErrors = new AtomicLong(0);
            this.verificationErrors = new AtomicLong(0);
            this.activeSessions = new AtomicLong(0);
        }
    }
    
    
    private final Map<String, SSLContext> contextCache;
    private final Map<String, SSLSessionInfo> sessionCache;
    
    
    private final SSLConfig config;
    
    
    private final SSLStats stats;
    
    
    private final ReentrantLock contextLock;
    private final ReentrantLock sessionLock;
    
    
    private final ScheduledExecutorService cleanupExecutor;
    
    
    private volatile X509TrustManager trustManager;
    
    
    private volatile X509KeyManager keyManager;
    
    


    public SSLContextManager(SSLConfig config) {
        this.config = config;
        this.contextCache = new ConcurrentHashMap<>();
        this.sessionCache = new ConcurrentHashMap<>();
        this.stats = new SSLStats();
        this.contextLock = new ReentrantLock();
        this.sessionLock = new ReentrantLock();
        this.cleanupExecutor = Executors.newSingleThreadScheduledExecutor();
        
        
        initializeManagers();
        
        
        startCleanupThread();
    }
    
    


    public SSLContextManager() {
        this(new SSLConfig());
    }
    
    


    private void initializeManagers() {
        try {
            
            TrustManagerFactory trustManagerFactory;
            if (config.trustStorePath != null) {
                KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
                try (InputStream trustStoreStream = new FileInputStream(config.trustStorePath)) {
                    trustStore.load(trustStoreStream, 
                        config.trustStorePassword != null ? 
                        config.trustStorePassword.toCharArray() : null);
                }
                trustManagerFactory = TrustManagerFactory.getInstance(
                    TrustManagerFactory.getDefaultAlgorithm());
                trustManagerFactory.init(trustStore);
            } else {
                trustManagerFactory = TrustManagerFactory.getInstance(
                    TrustManagerFactory.getDefaultAlgorithm());
                trustManagerFactory.init((KeyStore) null);
            }
            
            TrustManager[] trustManagers = trustManagerFactory.getTrustManagers();
            for (TrustManager tm : trustManagers) {
                if (tm instanceof X509TrustManager) {
                    this.trustManager = (X509TrustManager) tm;
                    break;
                }
            }
            
            
            if (config.keyStorePath != null) {
                KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
                try (InputStream keyStoreStream = new FileInputStream(config.keyStorePath)) {
                    keyStore.load(keyStoreStream, 
                        config.keyStorePassword != null ? 
                        config.keyStorePassword.toCharArray() : null);
                }
                KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(
                    KeyManagerFactory.getDefaultAlgorithm());
                keyManagerFactory.init(keyStore, 
                    config.keyPassword != null ? 
                    config.keyPassword.toCharArray() : null);
                
                KeyManager[] keyManagers = keyManagerFactory.getKeyManagers();
                for (KeyManager km : keyManagers) {
                    if (km instanceof X509KeyManager) {
                        this.keyManager = (X509KeyManager) km;
                        break;
                    }
                }
            }
            
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize SSL managers", e);
        }
    }
    
    


    private void startCleanupThread() {
        cleanupExecutor.scheduleAtFixedRate(() -> {
            cleanupExpiredSessions();
        }, 60000, 60000, TimeUnit.MILLISECONDS);
    }
    
    


    public SSLContext getSSLContext(String host, int port) throws SSLException {
        String contextKey = host + ":" + port;
        
        SSLContext context = contextCache.get(contextKey);
        if (context != null) {
            return context;
        }
        
        contextLock.lock();
        try {
            
            context = contextCache.get(contextKey);
            if (context != null) {
                return context;
            }
            
            
            context = createSSLContext(host, port);
            contextCache.put(contextKey, context);
            
            return context;
        } finally {
            contextLock.unlock();
        }
    }
    
    


    private SSLContext createSSLContext(String host, int port) throws SSLException {
        try {
            SSLContext context = SSLContext.getInstance(config.protocol.getProtocolName());
            
            
            TrustManager[] trustManagers;
            if (config.hostnameVerification && trustManager != null) {
                trustManagers = new TrustManager[] {
                    new HostnameVerifyingTrustManager(trustManager, host)
                };
            } else {
                trustManagers = new TrustManager[] { trustManager };
            }
            
            
            KeyManager[] keyManagers = keyManager != null ? 
                new KeyManager[] { keyManager } : null;
            
            context.init(keyManagers, trustManagers, new SecureRandom());
            
            
            SSLParameters sslParams = context.getDefaultSSLParameters();
            if (config.enabledCipherSuites != null) {
                sslParams.setCipherSuites(config.enabledCipherSuites);
            }
            if (config.enabledProtocols != null) {
                sslParams.setProtocols(config.enabledProtocols);
            }
            if (config.clientAuth) {
                sslParams.setNeedClientAuth(true);
            }
            
            return context;
        } catch (Exception e) {
            throw new SSLException("Failed to create SSL context", e);
        }
    }
    
    


    public SSLEngine createSSLEngine(String host, int port) throws SSLException {
        SSLContext context = getSSLContext(host, port);
        SSLEngine engine = context.createSSLEngine(host, port);
        engine.setUseClientMode(true);
        
        
        SSLParameters sslParams = engine.getSSLParameters();
        if (config.enabledCipherSuites != null) {
            sslParams.setCipherSuites(config.enabledCipherSuites);
        }
        if (config.enabledProtocols != null) {
            sslParams.setProtocols(config.enabledProtocols);
        }
        if (config.hostnameVerification) {
            sslParams.setEndpointIdentificationAlgorithm("HTTPS");
        }
        engine.setSSLParameters(sslParams);
        
        return engine;
    }
    
    


    public void recordSession(SSLEngine engine, String host, int port) {
        if (!config.enableSessionResumption) {
            return;
        }
        
        SSLSession session = engine.getSession();
        String sessionId = bytesToHex(session.getId());
        
        sessionLock.lock();
        try {
            SSLSessionInfo sessionInfo = new SSLSessionInfo(sessionId, session, host, port);
            sessionCache.put(sessionId, sessionInfo);
            stats.activeSessions.incrementAndGet();
            
            
            if (session.isValid()) {
                stats.sessionResumptions.incrementAndGet();
            } else {
                stats.sessionCreations.incrementAndGet();
            }
        } finally {
            sessionLock.unlock();
        }
    }
    
    


    public SSLSessionInfo getSessionInfo(String sessionId) {
        sessionLock.lock();
        try {
            return sessionCache.get(sessionId);
        } finally {
            sessionLock.unlock();
        }
    }
    
    


    private void cleanupExpiredSessions() {
        sessionLock.lock();
        try {
            Iterator<Map.Entry<String, SSLSessionInfo>> it = sessionCache.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<String, SSLSessionInfo> entry = it.next();
                SSLSessionInfo sessionInfo = entry.getValue();
                
                if (sessionInfo.isExpired() || !sessionInfo.valid) {
                    it.remove();
                    stats.activeSessions.decrementAndGet();
                }
            }
        } finally {
            sessionLock.unlock();
        }
    }
    
    


    public void recordHandshakeStart() {
        stats.totalHandshakes.incrementAndGet();
    }
    
    


    public void recordHandshakeSuccess() {
        stats.successfulHandshakes.incrementAndGet();
    }
    
    


    public void recordHandshakeFailure() {
        stats.failedHandshakes.incrementAndGet();
    }
    
    


    public void recordCertificateError() {
        stats.certificateErrors.incrementAndGet();
    }
    
    


    public void recordVerificationError() {
        stats.verificationErrors.incrementAndGet();
    }
    
    


    public SSLStats getStats() {
        return stats;
    }
    
    


    public SSLConfig getConfig() {
        return config;
    }
    
    


    public void clearContextCache() {
        contextLock.lock();
        try {
            contextCache.clear();
        } finally {
            contextLock.unlock();
        }
    }
    
    


    public void clearSessionCache() {
        sessionLock.lock();
        try {
            sessionCache.clear();
            stats.activeSessions.set(0);
        } finally {
            sessionLock.unlock();
        }
    }
    
    


    public void shutdown() {
        cleanupExecutor.shutdown();
        try {
            cleanupExecutor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        clearContextCache();
        clearSessionCache();
    }
    
    


    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }
    
    


    private static class HostnameVerifyingTrustManager implements X509TrustManager {
        private final X509TrustManager delegate;
        private final String expectedHostname;
        
        public HostnameVerifyingTrustManager(X509TrustManager delegate, String expectedHostname) {
            this.delegate = delegate;
            this.expectedHostname = expectedHostname;
        }
        
        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType) 
            throws CertificateException {
            delegate.checkClientTrusted(chain, authType);
        }
        
        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType) 
            throws CertificateException {
            delegate.checkServerTrusted(chain, authType);
            
            
            if (expectedHostname != null && chain.length > 0) {
                try {
                    verifyHostname(chain[0], expectedHostname);
                } catch (CertificateException e) {
                    throw e;
                }
            }
        }
        
        @Override
        public X509Certificate[] getAcceptedIssuers() {
            return delegate.getAcceptedIssuers();
        }
        
        


        private void verifyHostname(X509Certificate cert, String hostname) 
            throws CertificateException {
            String cn = getCommonName(cert);
            if (cn != null && cn.equals(hostname)) {
                return;
            }
            
            if (cn != null && cn.startsWith("*.") && 
                hostname.endsWith(cn.substring(1))) {
                return;
            }
            
            throw new CertificateException("Hostname verification failed");
        }
        
        


        private String getCommonName(X509Certificate cert) throws CertificateException {
            String dn = cert.getSubjectX500Principal().getName();
            String[] parts = dn.split(",");
            for (String part : parts) {
                part = part.trim();
                if (part.startsWith("CN=")) {
                    return part.substring(3);
                }
            }
            return null;
        }
    }
}
