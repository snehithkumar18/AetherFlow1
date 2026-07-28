package com.aetherflow;

import com.aetherflow.ConnectionPool;
import com.aetherflow.ConnectionPool.ConnectionEntry;





public class ConnectionPoolFuzzer {
    
    private static ConnectionPool connectionPool;
    
    static {
        connectionPool = new ConnectionPool();
    }
    
    


    public static void fuzzerTestOneInput(byte[] data) {
        try {
            if (connectionPool == null || data == null || data.length < 4) {
                return;
            }
            
            
            int offset = 0;
            
            while (offset < data.length - 4) {
                
                byte op = data[offset];
                offset++;
                
                switch (op) {
                    case 0: 
                        if (offset + 8 <= data.length) {
                            String host = "host_" + ((data[offset] & 0xFF));
                            int port = 1000 + ((data[offset + 1] & 0xFF));
                            offset += 8;
                            
                            ConnectionEntry entry = connectionPool.acquireConnection(host, port);
                            if (entry != null) {
                                entry.getConnectionId();
                                entry.getHost();
                                entry.getPort();
                                entry.getAge();
                                entry.getIdleTime();
                                entry.isExpired();
                                int count = entry.useCount;
                                int failures = entry.consecutiveFailures;
                            }
                        }
                        break;
                        
                    case 1: 
                        if (offset + 4 <= data.length) {
                            int connectionId = ((data[offset] & 0xFF) << 24) |
                                             ((data[offset + 1] & 0xFF) << 16) |
                                             ((data[offset + 2] & 0xFF) << 8) |
                                             (data[offset + 3] & 0xFF);
                            offset += 4;
                            
                            ConnectionEntry entry = connectionPool.getConnectionById(connectionId);
                            if (entry != null) {
                                connectionPool.releaseConnection(entry);
                            }
                        }
                        break;
                        
                    case 2: 
                        if (offset + 4 <= data.length) {
                            int connectionId = ((data[offset] & 0xFF) << 24) |
                                             ((data[offset + 1] & 0xFF) << 16) |
                                             ((data[offset + 2] & 0xFF) << 8) |
                                             (data[offset + 3] & 0xFF);
                            offset += 4;
                            
                            ConnectionEntry entry = connectionPool.getConnectionById(connectionId);
                            if (entry != null) {
                                connectionPool.evictConnection(entry);
                            }
                        }
                        break;
                        
                    case 3: 
                        if (offset + 4 <= data.length) {
                            int connectionId = ((data[offset] & 0xFF) << 24) |
                                             ((data[offset + 1] & 0xFF) << 16) |
                                             ((data[offset + 2] & 0xFF) << 8) |
                                             (data[offset + 3] & 0xFF);
                            offset += 4;
                            
                            ConnectionEntry entry = connectionPool.getConnectionById(connectionId);
                            if (entry != null) {
                                connectionPool.performHealthCheck(entry);
                            }
                        }
                        break;
                        
                    case 4: 
                        connectionPool.evictIdleConnections(5);
                        break;
                        
                    case 5: 
                        if (offset + 8 <= data.length) {
                            String host = "host_" + ((data[offset] & 0xFF));
                            int port = 1000 + ((data[offset + 1] & 0xFF));
                            offset += 8;
                            
                            connectionPool.getConnections(host, port);
                        }
                        break;
                        
                    case 6: 
                        ConnectionPool.getGlobalStaleConnection();
                        break;
                        
                    default:
                        offset++;
                        break;
                }
            }
            
            
            connectionPool.getStats();
            
        } catch (Exception e) {
            
        }
    }
}
