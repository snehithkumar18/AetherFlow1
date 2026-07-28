package com.aetherflow;

import java.util.Map;





public class AetherFlow {
    
    
    private final ConnectionPool connectionPool;
    private final SessionManager sessionManager;
    private final PacketAssembler packetAssembler;
    private final BufferPool bufferPool;
    private final CompressionLayer compressionLayer;
    private final EncryptionLayer encryptionLayer;
    private final RetryEngine retryEngine;
    private final MetricsCollector metricsCollector;
    private final ProtocolValidator protocolValidator;
    private final AsyncMessageQueue<ProtocolMessage> messageQueue;
    
    
    public static final String VERSION = "2.1.0";
    
    


    public AetherFlow() throws Exception {
        this.connectionPool = new ConnectionPool();
        this.sessionManager = new SessionManager();
        this.packetAssembler = new PacketAssembler();
        this.bufferPool = new BufferPool();
        this.compressionLayer = new CompressionLayer();
        this.encryptionLayer = new EncryptionLayer();
        this.retryEngine = new RetryEngine();
        this.metricsCollector = new MetricsCollector();
        this.protocolValidator = new ProtocolValidator();
        this.messageQueue = new AsyncMessageQueue<>();
        
        
        initializeMetrics();
    }
    
    


    private void initializeMetrics() {
        metricsCollector.registerCounter("nexus_messages_total", null);
        metricsCollector.registerCounter("nexus_bytes_sent", null);
        metricsCollector.registerCounter("nexus_bytes_received", null);
        metricsCollector.registerGauge("nexus_active_connections", null);
        metricsCollector.registerGauge("nexus_active_sessions", null);
        metricsCollector.registerHistogram("nexus_message_latency_ms", null);
    }
    
    


    public ConnectionPool getConnectionPool() {
        return connectionPool;
    }
    
    


    public SessionManager getSessionManager() {
        return sessionManager;
    }
    
    


    public PacketAssembler getPacketAssembler() {
        return packetAssembler;
    }
    
    


    public BufferPool getBufferPool() {
        return bufferPool;
    }
    
    


    public CompressionLayer getCompressionLayer() {
        return compressionLayer;
    }
    
    


    public EncryptionLayer getEncryptionLayer() {
        return encryptionLayer;
    }
    
    


    public RetryEngine getRetryEngine() {
        return retryEngine;
    }
    
    


    public MetricsCollector getMetricsCollector() {
        return metricsCollector;
    }
    
    


    public ProtocolValidator getProtocolValidator() {
        return protocolValidator;
    }
    
    


    public AsyncMessageQueue<ProtocolMessage> getMessageQueue() {
        return messageQueue;
    }
    
    


    public void processMessage(ProtocolMessage message) throws Exception {
        
        ProtocolValidator.ValidationResult result = protocolValidator.validate(message);
        if (!result.valid) {
            throw new Exception("Message validation failed: " + String.join(", ", result.errors));
        }
        
        
        metricsCollector.incrementCounter("nexus_messages_total", null);
        metricsCollector.incrementCounter("nexus_bytes_received", null, message.getPayloadLength());
        
        
        messageQueue.enqueue(message);
    }
    
    


    public ProtocolMessage createMessage(ProtocolMessage.MessageType type, byte[] payload) {
        ProtocolMessage message = new ProtocolMessage(type, payload);
        message.setChecksum(message.calculateChecksum());
        return message;
    }
    
    


    public byte[] serializeMessage(ProtocolMessage message) {
        byte[] serialized = message.serialize();
        
        
        metricsCollector.incrementCounter("nexus_bytes_sent", null, serialized.length);
        
        return serialized;
    }
    
    


    public ProtocolMessage deserializeMessage(byte[] data) {
        return ProtocolMessage.deserialize(data);
    }
    
    


    public Map<String, Object> getStatistics() {
        Map<String, Object> stats = new java.util.HashMap<>();
        
        stats.put("version", VERSION);
        stats.put("connectionPool", connectionPool.getStats());
        stats.put("sessionManager", sessionManager.getStats());
        stats.put("packetAssembler", packetAssembler.getStats());
        stats.put("bufferPool", bufferPool.getStats());
        stats.put("compressionLayer", compressionLayer.getStats());
        stats.put("encryptionLayer", encryptionLayer.getStats());
        stats.put("retryEngine", retryEngine.getAllStats());
        stats.put("metrics", metricsCollector.getSummary());
        stats.put("messageQueue", messageQueue.getStats());
        
        return stats;
    }
    
    


    public void shutdown() {
        messageQueue.shutdown();
        connectionPool.stopCleanupThread();
        sessionManager.stopCleanupThread();
        encryptionLayer.destroy();
    }
    
    


    public static void main(String[] args) {
        try {
            AetherFlow protocol = new AetherFlow();
            System.out.println("AetherFlow v" + VERSION + " initialized successfully");
            
            
            ProtocolMessage message = protocol.createMessage(
                ProtocolMessage.MessageType.HANDSHAKE,
                "Hello AetherFlow".getBytes()
            );
            
            System.out.println("Created message: " + message);
            
            
            byte[] serialized = protocol.serializeMessage(message);
            ProtocolMessage deserialized = protocol.deserializeMessage(serialized);
            
            System.out.println("Deserialized message: " + deserialized);
            
            
            protocol.shutdown();
            
            System.out.println("AetherFlow shutdown complete");
            
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
