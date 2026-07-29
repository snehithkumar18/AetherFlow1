package com.aetherflow;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;





public class ProtocolMessage {
    
    public enum MessageType {
        HANDSHAKE(0x01),
        AUTH(0x02),
        DATA(0x03),
        ACK(0x04),
        NACK(0x05),
        HEARTBEAT(0x06),
        SESSION_CREATE(0x07),
        SESSION_CLOSE(0x08),
        KEY_ROTATION(0x09),
        COMPRESSED_DATA(0x0A),
        ENCRYPTED_DATA(0x0B),
        FRAGMENT_START(0x0C),
        FRAGMENT_CONT(0x0D),
        FRAGMENT_END(0x0E),
        RETRANSMIT_REQUEST(0x0F),
        FLOW_CONTROL(0x10),
        ERROR(0x11),
        VARIANT(0x12),
        BATCH(0x13);
        
        private final byte code;
        
        MessageType(int code) {
            this.code = (byte) code;
        }
        
        public byte getCode() {
            return code;
        }
        
        public static MessageType fromCode(byte code) {
            for (MessageType type : values()) {
                if (type.code == code) {
                    return type;
                }
            }
            return null;
        }
    }
    
    
    public static final short PROTOCOL_VERSION_MAJOR = 2;
    public static final short PROTOCOL_VERSION_MINOR = 1;
    public static final int PROTOCOL_VERSION = (PROTOCOL_VERSION_MAJOR << 8) | PROTOCOL_VERSION_MINOR;
    
    
    public static final byte[] MAGIC_BYTES = new byte[] { 0x4E, 0x58, 0x50, 0x52 }; 
    
    
    private byte[] magic;
    private int protocolVersion;
    private MessageType messageType;
    private int sequenceNumber;
    private int sessionId;
    private int flags;
    private int payloadLength;
    private int checksum;
    private int timestamp;
    private byte[] payload;
    
    
    private boolean compressionEnabled;
    private boolean encryptionEnabled;
    private boolean fragmentationEnabled;
    private boolean retransmissionEnabled;
    
    
    private Map<String, String> extendedHeaders;
    private int fragmentOffset;
    private int totalFragmentSize;
    private int fragmentIndex;
    private int totalFragments;
    
    


    public ProtocolMessage() {
        this.magic = MAGIC_BYTES.clone();
        this.protocolVersion = PROTOCOL_VERSION;
        this.sequenceNumber = 0;
        this.sessionId = 0;
        this.flags = 0;
        this.payloadLength = 0;
        this.checksum = 0;
        this.timestamp = (int) (System.currentTimeMillis() / 1000);
        this.payload = new byte[0];
        this.extendedHeaders = new HashMap<>();
        this.fragmentOffset = 0;
        this.totalFragmentSize = 0;
        this.fragmentIndex = 0;
        this.totalFragments = 0;
    }
    
    


    public ProtocolMessage(MessageType messageType) {
        this();
        this.messageType = messageType;
    }
    
    


    public ProtocolMessage(MessageType messageType, byte[] payload) {
        this(messageType);
        this.payload = payload != null ? payload.clone() : new byte[0];
        this.payloadLength = this.payload.length;
    }
    
    
    public byte[] getMagic() {
        return magic.clone();
    }
    
    public void setMagic(byte[] magic) {
        this.magic = magic != null ? magic.clone() : MAGIC_BYTES.clone();
    }
    
    public int getProtocolVersion() {
        return protocolVersion;
    }
    
    public void setProtocolVersion(int protocolVersion) {
        this.protocolVersion = protocolVersion;
    }
    
    public MessageType getMessageType() {
        return messageType;
    }
    
    public void setMessageType(MessageType messageType) {
        this.messageType = messageType;
    }
    
    public int getSequenceNumber() {
        return sequenceNumber;
    }
    
    public void setSequenceNumber(int sequenceNumber) {
        this.sequenceNumber = sequenceNumber;
    }
    
    public int getSessionId() {
        return sessionId;
    }
    
    public void setSessionId(int sessionId) {
        this.sessionId = sessionId;
    }
    
    public int getFlags() {
        return flags;
    }
    
    public void setFlags(int flags) {
        this.flags = flags;
    }
    
    public int getPayloadLength() {
        return payloadLength;
    }
    
    public void setPayloadLength(int payloadLength) {
        this.payloadLength = payloadLength;
    }
    
    public int getChecksum() {
        return checksum;
    }
    
    public void setChecksum(int checksum) {
        this.checksum = checksum;
    }
    
    public int getTimestamp() {
        return timestamp;
    }
    
    public void setTimestamp(int timestamp) {
        this.timestamp = timestamp;
    }
    
    public byte[] getPayload() {
        return payload.clone();
    }
    
    public void setPayload(byte[] payload) {
        this.payload = payload != null ? payload.clone() : new byte[0];
        this.payloadLength = this.payload.length;
    }
    
    public boolean isCompressionEnabled() {
        return compressionEnabled;
    }
    
    public void setCompressionEnabled(boolean compressionEnabled) {
        this.compressionEnabled = compressionEnabled;
        updateFlags();
    }
    
    public boolean isEncryptionEnabled() {
        return encryptionEnabled;
    }
    
    public void setEncryptionEnabled(boolean encryptionEnabled) {
        this.encryptionEnabled = encryptionEnabled;
        updateFlags();
    }
    
    public boolean isFragmentationEnabled() {
        return fragmentationEnabled;
    }
    
    public void setFragmentationEnabled(boolean fragmentationEnabled) {
        this.fragmentationEnabled = fragmentationEnabled;
        updateFlags();
    }
    
    public boolean isRetransmissionEnabled() {
        return retransmissionEnabled;
    }
    
    public void setRetransmissionEnabled(boolean retransmissionEnabled) {
        this.retransmissionEnabled = retransmissionEnabled;
        updateFlags();
    }
    
    public Map<String, String> getExtendedHeaders() {
        return new HashMap<>(extendedHeaders);
    }
    
    public void setExtendedHeaders(Map<String, String> extendedHeaders) {
        this.extendedHeaders = extendedHeaders != null ? new HashMap<>(extendedHeaders) : new HashMap<>();
    }
    
    public void addExtendedHeader(String key, String value) {
        this.extendedHeaders.put(key, value);
    }
    
    public String getExtendedHeader(String key) {
        return this.extendedHeaders.get(key);
    }
    
    public int getFragmentOffset() {
        return fragmentOffset;
    }
    
    public void setFragmentOffset(int fragmentOffset) {
        this.fragmentOffset = fragmentOffset;
    }
    
    public int getTotalFragmentSize() {
        return totalFragmentSize;
    }
    
    public void setTotalFragmentSize(int totalFragmentSize) {
        this.totalFragmentSize = totalFragmentSize;
    }
    
    public int getFragmentIndex() {
        return fragmentIndex;
    }
    
    public void setFragmentIndex(int fragmentIndex) {
        this.fragmentIndex = fragmentIndex;
    }
    
    public int getTotalFragments() {
        return totalFragments;
    }
    
    public void setTotalFragments(int totalFragments) {
        this.totalFragments = totalFragments;
    }
    
    


    private void updateFlags() {
        int newFlags = 0;
        if (compressionEnabled) {
            newFlags |= 0x01;
        }
        if (encryptionEnabled) {
            newFlags |= 0x02;
        }
        if (fragmentationEnabled) {
            newFlags |= 0x04;
        }
        if (retransmissionEnabled) {
            newFlags |= 0x08;
        }
        this.flags = newFlags;
    }
    
    


    public void parseFlags() {
        this.compressionEnabled = (flags & 0x01) != 0;
        this.encryptionEnabled = (flags & 0x02) != 0;
        this.fragmentationEnabled = (flags & 0x04) != 0;
        this.retransmissionEnabled = (flags & 0x08) != 0;
    }
    
    


    public int calculateChecksum() {
        int sum = 0;
        
        
        for (byte b : magic) {
            sum += (b & 0xFF);
        }
        
        
        sum += (protocolVersion >> 8) & 0xFF;
        sum += protocolVersion & 0xFF;
        
        
        if (messageType != null) {
            sum += messageType.getCode() & 0xFF;
        }
        
        
        sum += (sequenceNumber >> 24) & 0xFF;
        sum += (sequenceNumber >> 16) & 0xFF;
        sum += (sequenceNumber >> 8) & 0xFF;
        sum += sequenceNumber & 0xFF;
        
        
        sum += (sessionId >> 24) & 0xFF;
        sum += (sessionId >> 16) & 0xFF;
        sum += (sessionId >> 8) & 0xFF;
        sum += sessionId & 0xFF;
        
        
        sum += (flags >> 24) & 0xFF;
        sum += (flags >> 16) & 0xFF;
        sum += (flags >> 8) & 0xFF;
        sum += flags & 0xFF;
        
        
        sum += (payloadLength >> 24) & 0xFF;
        sum += (payloadLength >> 16) & 0xFF;
        sum += (payloadLength >> 8) & 0xFF;
        sum += payloadLength & 0xFF;
        
        
        sum += (timestamp >> 24) & 0xFF;
        sum += (timestamp >> 16) & 0xFF;
        sum += (timestamp >> 8) & 0xFF;
        sum += timestamp & 0xFF;
        
        
        for (byte b : payload) {
            sum += (b & 0xFF);
        }
        
        return sum & 0xFFFFFFFF;
    }
    
    


    public boolean validate() {
        
        if (magic == null || magic.length != MAGIC_BYTES.length) {
            return false;
        }
        for (int i = 0; i < MAGIC_BYTES.length; i++) {
            if (magic[i] != MAGIC_BYTES[i]) {
                return false;
            }
        }
        
        
        if (protocolVersion != PROTOCOL_VERSION) {
            return false;
        }
        
        
        if (messageType == null) {
            return false;
        }
        
        
        if (payloadLength != payload.length) {
            return false;
        }
        
        
        if (payloadLength < 0 || payloadLength > 16 * 1024 * 1024) { 
            return false;
        }
        
        
        int calculatedChecksum = calculateChecksum();
        if (checksum != 0 && checksum != calculatedChecksum) {
            return false;
        }
        
        
        if (fragmentationEnabled) {
            if (fragmentIndex < 0 || fragmentIndex >= totalFragments) {
                return false;
            }
            if (totalFragments <= 0 || totalFragments > 1000) {
                return false;
            }
            if (fragmentOffset < 0 || fragmentOffset >= totalFragmentSize) {
                return false;
            }
            if (totalFragmentSize <= 0 || totalFragmentSize > 16 * 1024 * 1024) {
                return false;
            }
            
            if (fragmentIndex == totalFragments - 1 && fragmentOffset + payloadLength != totalFragmentSize) {
                return false;
            }
        }
        
        return true;
    }
    
    


    public byte[] serialize() {
        
        int headerSize = 4 + 
                        2 + 
                        1 + 
                        4 + 
                        4 + 
                        4 + 
                        4 + 
                        4 + 
                        4 + 
                        4 + 
                        4 + 
                        2 + 
                        2;  
        
        int extendedHeaderSize = 0;
        for (Map.Entry<String, String> entry : extendedHeaders.entrySet()) {
            extendedHeaderSize += 2 + entry.getKey().getBytes().length; 
            extendedHeaderSize += 2 + entry.getValue().getBytes().length; 
        }
        extendedHeaderSize += 2; 
        
        int totalSize = headerSize + extendedHeaderSize + payloadLength;
        if (totalSize < 0 || totalSize > 32 * 1024 * 1024) {
            throw new IllegalArgumentException("Message size exceeds maximum limit: " + totalSize);
        }
        
        ByteBuffer buffer = ByteBuffer.allocate(totalSize);
        buffer.order(ByteOrder.BIG_ENDIAN);
        
        
        buffer.put(magic);
        
        
        buffer.putShort((short) protocolVersion);
        
        
        buffer.put(messageType.getCode());
        
        
        buffer.putInt(sequenceNumber);
        
        
        buffer.putInt(sessionId);
        
        
        buffer.putInt(flags);
        
        
        buffer.putInt(payloadLength);
        
        
        buffer.putInt(checksum);
        
        
        buffer.putInt(timestamp);
        
        
        buffer.putInt(fragmentOffset);
        buffer.putInt(totalFragmentSize);
        buffer.putShort((short) fragmentIndex);
        buffer.putShort((short) totalFragments);
        
        
        buffer.putShort((short) extendedHeaders.size());
        for (Map.Entry<String, String> entry : extendedHeaders.entrySet()) {
            byte[] keyBytes = entry.getKey().getBytes();
            byte[] valueBytes = entry.getValue().getBytes();
            buffer.putShort((short) keyBytes.length);
            buffer.put(keyBytes);
            buffer.putShort((short) valueBytes.length);
            buffer.put(valueBytes);
        }
        
        
        buffer.put(payload);
        
        return buffer.array();
    }
    
    


    public static ProtocolMessage deserialize(byte[] data) {
        if (data == null || data.length < 35) { 
            return null;
        }
        
        ByteBuffer buffer = ByteBuffer.wrap(data);
        buffer.order(ByteOrder.BIG_ENDIAN);
        
        ProtocolMessage message = new ProtocolMessage();
        
        
        byte[] magic = new byte[4];
        buffer.get(magic);
        message.setMagic(magic);
        
        
        message.setProtocolVersion(buffer.getShort() & 0xFFFF);
        
        
        byte messageTypeCode = buffer.get();
        message.setMessageType(MessageType.fromCode(messageTypeCode));
        
        
        message.setSequenceNumber(buffer.getInt());
        
        
        message.setSessionId(buffer.getInt());
        
        
        message.setFlags(buffer.getInt());
        message.parseFlags();
        
        
        int payloadLength = buffer.getInt();
        if (payloadLength < 0 || payloadLength > 16 * 1024 * 1024 || payloadLength > buffer.remaining()) {
            return null;
        }
        message.setPayloadLength(payloadLength);
        
        
        message.setChecksum(buffer.getInt());
        
        
        message.setTimestamp(buffer.getInt());
        
        
        message.setFragmentOffset(buffer.getInt());
        message.setTotalFragmentSize(buffer.getInt());
        message.setFragmentIndex(buffer.getShort() & 0xFFFF);
        message.setTotalFragments(buffer.getShort() & 0xFFFF);
        
        
        int headerCount = buffer.getShort() & 0xFFFF;
        Map<String, String> headers = new HashMap<>();
        for (int i = 0; i < headerCount; i++) {
            int keyLength = buffer.getShort() & 0xFFFF;
            byte[] keyBytes = new byte[keyLength];
            buffer.get(keyBytes);
            String key = new String(keyBytes);
            
            int valueLength = buffer.getShort() & 0xFFFF;
            byte[] valueBytes = new byte[valueLength];
            buffer.get(valueBytes);
            String value = new String(valueBytes);
            
            headers.put(key, value);
        }
        message.setExtendedHeaders(headers);
        
        
        if (message.payloadLength > 0 && buffer.remaining() >= message.payloadLength) {
            byte[] payload = new byte[message.payloadLength];
            buffer.get(payload);
            message.setPayload(payload);
        }
        
        return message;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ProtocolMessage that = (ProtocolMessage) o;
        return protocolVersion == that.protocolVersion &&
               sequenceNumber == that.sequenceNumber &&
               sessionId == that.sessionId &&
               flags == that.flags &&
               payloadLength == that.payloadLength &&
               checksum == that.checksum &&
               timestamp == that.timestamp &&
               fragmentOffset == that.fragmentOffset &&
               totalFragmentSize == that.totalFragmentSize &&
               fragmentIndex == that.fragmentIndex &&
               totalFragments == that.totalFragments &&
               Arrays.equals(magic, that.magic) &&
               messageType == that.messageType &&
               Arrays.equals(payload, that.payload) &&
               Objects.equals(extendedHeaders, that.extendedHeaders);
    }
    
    @Override
    public int hashCode() {
        int result = Objects.hash(protocolVersion, messageType, sequenceNumber, sessionId, flags, 
                                 payloadLength, checksum, timestamp, extendedHeaders, 
                                 fragmentOffset, totalFragmentSize, fragmentIndex, totalFragments);
        result = 31 * result + Arrays.hashCode(magic);
        result = 31 * result + Arrays.hashCode(payload);
        return result;
    }
    
    @Override
    public String toString() {
        return "ProtocolMessage{" +
               "messageType=" + messageType +
               ", sequenceNumber=" + sequenceNumber +
               ", sessionId=" + sessionId +
               ", payloadLength=" + payloadLength +
               ", flags=" + flags +
               ", timestamp=" + timestamp +
               ", fragmentIndex=" + fragmentIndex +
               ", totalFragments=" + totalFragments +
               '}';
    }
}
