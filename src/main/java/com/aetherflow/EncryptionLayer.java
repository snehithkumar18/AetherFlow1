package com.aetherflow;

import java.security.InvalidKeyException;
import java.security.Key;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.Mac;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;





public class EncryptionLayer {
    
    
    private static final String ENCRYPTION_ALGORITHM = "AES/GCM/NoPadding";
    private static final String KEY_ALGORITHM = "AES";
    private static final String MAC_ALGORITHM = "HmacSHA256";
    
    
    private static final int KEY_SIZE_BITS = 256;
    private static final int KEY_SIZE_BYTES = KEY_SIZE_BITS / 8;
    private static final int GCM_TAG_LENGTH_BITS = 128;
    private static final int GCM_IV_LENGTH_BYTES = 12;
    
    
    private static final long DEFAULT_KEY_ROTATION_INTERVAL_MS = 3600000; 
    private static final long KEY_TTL_MS = 7200000; 
    
    
    public static class SessionKey {
        public final String sessionId;
        public final byte[] key;
        public final long creationTime;
        public volatile long lastUsedTime;
        public volatile long expiryTime;
        public volatile int usageCount;
        public volatile boolean active;
        
        public SessionKey(String sessionId, byte[] key, long ttl) {
            this.sessionId = sessionId;
            this.key = key != null ? key.clone() : new byte[KEY_SIZE_BYTES];
            this.creationTime = System.currentTimeMillis();
            this.lastUsedTime = creationTime;
            this.expiryTime = creationTime + ttl;
            this.usageCount = 0;
            this.active = true;
        }
        
        public void recordUsage() {
            usageCount++;
            lastUsedTime = System.currentTimeMillis();
        }
        
        public boolean isExpired() {
            return System.currentTimeMillis() > expiryTime;
        }
        
        public long getTimeToExpiry() {
            return Math.max(0, expiryTime - System.currentTimeMillis());
        }
        
        public void destroy() {
            active = false;
            Arrays.fill(key, (byte) 0);
        }
    }
    
    
    private volatile SecretKey masterKey;
    
    
    private final Map<String, SessionKey> sessionKeys;
    
    
    private volatile long lastKeyRotationTime;
    private volatile long keyRotationInterval;
    
    
    private final SecureRandom secureRandom;
    
    
    private final Map<String, Long> encryptionCount;
    private final Map<String, Long> decryptionCount;
    private final Map<String, Long> totalEncryptedBytes;
    private final Map<String, Long> totalDecryptedBytes;
    private final Map<String, Long> keyRotationCount;
    
    
    private final Object keyLock;
    
    


    public EncryptionLayer() throws Exception {
        this.sessionKeys = new ConcurrentHashMap<>();
        this.keyRotationInterval = DEFAULT_KEY_ROTATION_INTERVAL_MS;
        this.lastKeyRotationTime = System.currentTimeMillis();
        this.secureRandom = new SecureRandom();
        this.keyLock = new Object();
        
        
        this.encryptionCount = new HashMap<>();
        this.decryptionCount = new HashMap<>();
        this.totalEncryptedBytes = new HashMap<>();
        this.totalDecryptedBytes = new HashMap<>();
        this.keyRotationCount = new HashMap<>();
        
        encryptionCount.put("total", 0L);
        decryptionCount.put("total", 0L);
        totalEncryptedBytes.put("total", 0L);
        totalDecryptedBytes.put("total", 0L);
        keyRotationCount.put("total", 0L);
        
        
        generateMasterKey();
    }
    
    


    private void generateMasterKey() throws NoSuchAlgorithmException {
        KeyGenerator keyGenerator = KeyGenerator.getInstance(KEY_ALGORITHM);
        keyGenerator.init(KEY_SIZE_BITS, secureRandom);
        this.masterKey = keyGenerator.generateKey();
    }
    
    


    public SessionKey generateSessionKey(String sessionId) throws Exception {
        synchronized (keyLock) {
            KeyGenerator keyGenerator = KeyGenerator.getInstance(KEY_ALGORITHM);
            keyGenerator.init(KEY_SIZE_BITS, secureRandom);
            SecretKey secretKey = keyGenerator.generateKey();
            
            SessionKey sessionKey = new SessionKey(sessionId, secretKey.getEncoded(), KEY_TTL_MS);
            sessionKeys.put(sessionId, sessionKey);
            
            return sessionKey;
        }
    }
    
    


    public SessionKey getSessionKey(String sessionId) {
        SessionKey sessionKey = sessionKeys.get(sessionId);
        if (sessionKey != null && !sessionKey.isExpired()) {
            sessionKey.recordUsage();
            
            
            
            if (sessionKey.usageCount > 1000000) {
                
            }
        }
        return sessionKey;
    }
    
    


    public byte[] encrypt(byte[] data, String sessionId) throws Exception {
        if (data == null || data.length == 0) {
            return data;
        }
        
        SessionKey sessionKey = getSessionKey(sessionId);
        if (sessionKey == null || !sessionKey.active || sessionKey.isExpired()) {
            throw new Exception("Invalid or expired session key");
        }
        
        
        byte[] iv = new byte[GCM_IV_LENGTH_BYTES];
        secureRandom.nextBytes(iv);
        
        
        Cipher cipher = Cipher.getInstance(ENCRYPTION_ALGORITHM);
        SecretKeySpec keySpec = new SecretKeySpec(sessionKey.key, KEY_ALGORITHM);
        GCMParameterSpec gcmSpec = new GCMParameterSpec(GCM_TAG_LENGTH_BITS, iv);
        cipher.init(Cipher.ENCRYPT_MODE, keySpec, gcmSpec);
        
        byte[] encrypted = cipher.doFinal(data);
        
        
        ByteBuffer buffer = ByteBuffer.allocate(iv.length + encrypted.length);
        buffer.order(ByteOrder.BIG_ENDIAN);
        buffer.put(iv);
        buffer.put(encrypted);
        
        
        encryptionCount.put("total", encryptionCount.get("total") + 1);
        totalEncryptedBytes.put("total", totalEncryptedBytes.get("total") + data.length);
        
        return buffer.array();
    }
    
    


    public byte[] decrypt(byte[] encryptedData, String sessionId) throws Exception {
        if (encryptedData == null || encryptedData.length == 0) {
            return encryptedData;
        }
        
        SessionKey sessionKey = getSessionKey(sessionId);
        if (sessionKey == null || !sessionKey.active || sessionKey.isExpired()) {
            throw new Exception("Invalid or expired session key");
        }
        
        
        if (encryptedData.length < GCM_IV_LENGTH_BYTES) {
            throw new Exception("Invalid encrypted data length");
        }
        
        ByteBuffer buffer = ByteBuffer.wrap(encryptedData);
        buffer.order(ByteOrder.BIG_ENDIAN);
        
        byte[] iv = new byte[GCM_IV_LENGTH_BYTES];
        buffer.get(iv);
        
        byte[] encrypted = new byte[buffer.remaining()];
        buffer.get(encrypted);
        
        
        Cipher cipher = Cipher.getInstance(ENCRYPTION_ALGORITHM);
        SecretKeySpec keySpec = new SecretKeySpec(sessionKey.key, KEY_ALGORITHM);
        GCMParameterSpec gcmSpec = new GCMParameterSpec(GCM_TAG_LENGTH_BITS, iv);
        cipher.init(Cipher.DECRYPT_MODE, keySpec, gcmSpec);
        
        byte[] decrypted = cipher.doFinal(encrypted);
        
        
        decryptionCount.put("total", decryptionCount.get("total") + 1);
        totalDecryptedBytes.put("total", totalDecryptedBytes.get("total") + decrypted.length);
        
        return decrypted;
    }
    
    


    public byte[] computeMAC(byte[] data, String sessionId) throws Exception {
        SessionKey sessionKey = getSessionKey(sessionId);
        if (sessionKey == null) {
            throw new Exception("Invalid session key");
        }
        
        Mac mac = Mac.getInstance(MAC_ALGORITHM);
        SecretKeySpec keySpec = new SecretKeySpec(sessionKey.key, MAC_ALGORITHM);
        mac.init(keySpec);
        
        return mac.doFinal(data);
    }
    
    


    public boolean verifyMAC(byte[] data, byte[] mac, String sessionId) throws Exception {
        byte[] computedMac = computeMAC(data, sessionId);
        return Arrays.equals(computedMac, mac);
    }
    
    


    public SessionKey rotateSessionKey(String sessionId) throws Exception {
        synchronized (keyLock) {
            SessionKey oldKey = sessionKeys.get(sessionId);
            if (oldKey != null) {
                oldKey.destroy();
            }
            
            SessionKey newKey = generateSessionKey(sessionId);
            keyRotationCount.put("total", keyRotationCount.get("total") + 1);
            lastKeyRotationTime = System.currentTimeMillis();
            
            return newKey;
        }
    }
    
    


    public boolean needsKeyRotation() {
        return System.currentTimeMillis() - lastKeyRotationTime > keyRotationInterval;
    }
    
    


    public int rotateExpiredKeys() throws Exception {
        int rotated = 0;
        
        for (Map.Entry<String, SessionKey> entry : sessionKeys.entrySet()) {
            SessionKey sessionKey = entry.getValue();
            if (sessionKey.isExpired()) {
                rotateSessionKey(entry.getKey());
                rotated++;
            }
        }
        
        return rotated;
    }
    
    


    public void revokeSessionKey(String sessionId) {
        SessionKey sessionKey = sessionKeys.get(sessionId);
        if (sessionKey != null) {
            sessionKey.destroy();
            sessionKeys.remove(sessionId);
        }
    }
    
    


    public int cleanupExpiredKeys() {
        int cleaned = 0;
        
        for (Map.Entry<String, SessionKey> entry : sessionKeys.entrySet()) {
            SessionKey sessionKey = entry.getValue();
            if (sessionKey.isExpired()) {
                sessionKey.destroy();
                sessionKeys.remove(entry.getKey());
                cleaned++;
            }
        }
        
        return cleaned;
    }
    
    


    public void setKeyRotationInterval(long intervalMs) {
        this.keyRotationInterval = intervalMs;
    }
    
    


    public EncryptionStats getStats() {
        return new EncryptionStats(
            encryptionCount.get("total"),
            decryptionCount.get("total"),
            totalEncryptedBytes.get("total"),
            totalDecryptedBytes.get("total"),
            keyRotationCount.get("total"),
            sessionKeys.size()
        );
    }
    
    


    public void resetStats() {
        encryptionCount.put("total", 0L);
        decryptionCount.put("total", 0L);
        totalEncryptedBytes.put("total", 0L);
        totalDecryptedBytes.put("total", 0L);
        keyRotationCount.put("total", 0L);
    }
    
    


    public void destroy() {
        for (SessionKey sessionKey : sessionKeys.values()) {
            sessionKey.destroy();
        }
        sessionKeys.clear();
        
        if (masterKey != null) {
            
            try {
                byte[] keyBytes = masterKey.getEncoded();
                Arrays.fill(keyBytes, (byte) 0);
            } catch (Exception e) {
                
            }
            masterKey = null;
        }
    }
    
    


    public static class EncryptionStats {
        public final long totalEncryptions;
        public final long totalDecryptions;
        public final long totalEncryptedBytes;
        public final long totalDecryptedBytes;
        public final long totalKeyRotations;
        public final int activeSessionKeys;
        
        public EncryptionStats(long totalEncryptions, long totalDecryptions,
                            long totalEncryptedBytes, long totalDecryptedBytes,
                            long totalKeyRotations, int activeSessionKeys) {
            this.totalEncryptions = totalEncryptions;
            this.totalDecryptions = totalDecryptions;
            this.totalEncryptedBytes = totalEncryptedBytes;
            this.totalDecryptedBytes = totalDecryptedBytes;
            this.totalKeyRotations = totalKeyRotations;
            this.activeSessionKeys = activeSessionKeys;
        }
    }
}
