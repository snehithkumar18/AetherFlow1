package com.aetherflow;

import com.aetherflow.SessionManager;
import com.aetherflow.SessionManager.SessionEntry;

public class SessionManagerFuzzer {
    
    private static SessionManager sessionManager;
    
    static {
        sessionManager = new SessionManager();
    }
    
    public static void fuzzerTestOneInput(byte[] data) {
        if (data == null || data.length < 4) {
            return;
        }

        try {
            int offset = 0;
            while (offset < data.length - 4) {
                byte op = data[offset];
                offset++;
                
                switch (op) {
                    case 0: 
                        if (offset + 8 <= data.length) {
                            String userId = "user_" + ((data[offset] & 0xFF));
                            String authToken = "token_" + ((data[offset + 1] & 0xFF));
                            offset += 8;
                            
                            SessionEntry session = sessionManager.createSession(userId, authToken);
                            if (session != null) {
                                session.getSessionId();
                                session.getUserId();
                                session.isExpired();
                                session.getAge();
                                session.getIdleTime();
                            }
                        }
                        break;
                        
                    case 1: 
                        if (offset + 16 <= data.length) {
                            String sessionId = "sess_" + ((data[offset] & 0xFF));
                            offset += 16;
                            
                            boolean valid = sessionManager.validateSession(sessionId);
                            SessionEntry session = sessionManager.getSession(sessionId);
                            if (session != null) {
                                session.recordAccess();
                            }
                        }
                        break;
                        
                    case 2: 
                        if (offset + 16 <= data.length) {
                            String sessionId = "sess_" + ((data[offset] & 0xFF));
                            String role = "role_" + ((data[offset + 1] & 0xFF));
                            offset += 16;
                            
                            sessionManager.authenticateSession(sessionId, role);
                        }
                        break;
                        
                    case 3: 
                        if (offset + 16 <= data.length) {
                            String sessionId = "sess_" + ((data[offset] & 0xFF));
                            String permission = "perm_" + ((data[offset + 1] & 0xFF));
                            offset += 16;
                            
                            sessionManager.grantPermission(sessionId, permission);
                        }
                        break;
                        
                    case 4: 
                        if (offset + 16 <= data.length) {
                            String sessionId = "sess_" + ((data[offset] & 0xFF));
                            offset += 16;
                            
                            sessionManager.refreshSession(sessionId);
                        }
                        break;
                        
                    case 5: 
                        if (offset + 16 <= data.length) {
                            String sessionId = "sess_" + ((data[offset] & 0xFF));
                            offset += 16;
                            
                            sessionManager.revokeSession(sessionId);
                        }
                        break;
                        
                    case 6: 
                        CacheManager cache = new CacheManager(new CacheManager.CacheConfig());
                        String cacheKey = "key_" + ((data[offset] & 0xFF));
                        byte[] cacheValue = new byte[data[offset + 1] & 0xFF];
                        cache.put(cacheKey, cacheValue);
                        cache.get(cacheKey);
                        offset += 4;
                        break;
                        
                    case 7: 
                        SSLContextManager ssl = new SSLContextManager(new SSLContextManager.SSLConfig());
                        ssl.getSSLContext("host_" + ((data[offset] & 0xFF)), 443);
                        offset += 4;
                        break;
                        
                    default:
                        offset++;
                        break;
                }
            }
            sessionManager.getStats();
        } catch (RuntimeException e) {
            throw e; 
        } catch (Exception e) {
            
        }
    }
}
