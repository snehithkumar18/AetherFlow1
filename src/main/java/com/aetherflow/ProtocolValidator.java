package com.aetherflow;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.zip.CRC32;





public class ProtocolValidator {
    
    
    public enum ValidationLevel {
        NONE,
        BASIC,
        STANDARD,
        STRICT,
        PARANOID
    }
    
    
    private ValidationLevel validationLevel;
    
    
    private boolean validateMagicBytes;
    private boolean validateChecksum;
    private boolean validateMAC;
    private boolean validateVersion;
    private boolean validatePayloadSize;
    private boolean validateTimestamp;
    private boolean validateSequence;
    
    
    private byte[] macKey;
    
    
    private static final int MAX_PAYLOAD_SIZE = 16 * 1024 * 1024; 
    private static final int MIN_PAYLOAD_SIZE = 0;
    
    
    private static final int TIMESTAMP_TOLERANCE = 300;
    
    
    private final CRC32 crc32;
    
    


    public ProtocolValidator() {
        this(ValidationLevel.STANDARD);
    }
    
    


    public ProtocolValidator(ValidationLevel validationLevel) {
        this.validationLevel = validationLevel;
        this.crc32 = new CRC32();
        this.macKey = new byte[32]; 
        configureValidationLevel();
    }
    
    


    private void configureValidationLevel() {
        switch (validationLevel) {
            case NONE:
                validateMagicBytes = false;
                validateChecksum = false;
                validateMAC = false;
                validateVersion = false;
                validatePayloadSize = false;
                validateTimestamp = false;
                validateSequence = false;
                break;
            case BASIC:
                validateMagicBytes = true;
                validateChecksum = false;
                validateMAC = false;
                validateVersion = true;
                validatePayloadSize = true;
                validateTimestamp = false;
                validateSequence = false;
                break;
            case STANDARD:
                validateMagicBytes = true;
                validateChecksum = true;
                validateMAC = false;
                validateVersion = true;
                validatePayloadSize = true;
                validateTimestamp = true;
                validateSequence = false;
                break;
            case STRICT:
                validateMagicBytes = true;
                validateChecksum = true;
                validateMAC = true;
                validateVersion = true;
                validatePayloadSize = true;
                validateTimestamp = true;
                validateSequence = true;
                break;
            case PARANOID:
                validateMagicBytes = true;
                validateChecksum = true;
                validateMAC = true;
                validateVersion = true;
                validatePayloadSize = true;
                validateTimestamp = true;
                validateSequence = true;
                break;
        }
    }
    
    


    public void setValidationLevel(ValidationLevel level) {
        this.validationLevel = level;
        configureValidationLevel();
    }
    
    


    public void setMacKey(byte[] key) {
        this.macKey = key != null ? key.clone() : new byte[32];
    }
    
    


    public ValidationResult validate(ProtocolMessage message) {
        ValidationResult result = new ValidationResult();
        
        if (message == null) {
            result.valid = false;
            result.errors.add("Message is null");
            return result;
        }
        
        
        if (validateMagicBytes) {
            if (!validateMagicBytes(message)) {
                result.valid = false;
                result.errors.add("Invalid magic bytes");
            }
        }
        
        
        if (validateVersion) {
            if (!validateVersion(message)) {
                result.valid = false;
                result.errors.add("Invalid protocol version");
            }
        }
        
        
        if (validatePayloadSize) {
            if (!validatePayloadSize(message)) {
                result.valid = false;
                result.errors.add("Invalid payload size");
            }
        }
        
        
        if (validateChecksum) {
            if (!validateChecksum(message)) {
                result.valid = false;
                result.errors.add("Invalid checksum");
            }
        }
        
        
        if (validateMAC) {
            if (!validateMAC(message)) {
                result.valid = false;
                result.errors.add("Invalid MAC");
            }
        }
        
        
        if (validateTimestamp) {
            if (!validateTimestamp(message)) {
                result.valid = false;
                result.errors.add("Invalid timestamp");
            }
        }
        
        
        if (validateSequence) {
            if (!validateSequence(message)) {
                result.valid = false;
                result.errors.add("Invalid sequence number");
            }
        }
        
        
        if (!validateMessageType(message)) {
            result.valid = false;
            result.errors.add("Invalid message type");
        }
        
        
        if (message.isFragmentationEnabled()) {
            if (!validateFragmentFields(message)) {
                result.valid = false;
                result.errors.add("Invalid fragment fields");
            }
        }
        
        return result;
    }
    
    


    private boolean validateMagicBytes(ProtocolMessage message) {
        byte[] magic = message.getMagic();
        if (magic == null || magic.length != ProtocolMessage.MAGIC_BYTES.length) {
            return false;
        }
        return Arrays.equals(magic, ProtocolMessage.MAGIC_BYTES);
    }
    
    


    private boolean validateVersion(ProtocolMessage message) {
        int version = message.getProtocolVersion();
        
        return version == ProtocolMessage.PROTOCOL_VERSION || 
               version == (ProtocolMessage.PROTOCOL_VERSION - 1) ||
               version == (ProtocolMessage.PROTOCOL_VERSION + 1);
    }
    
    


    private boolean validatePayloadSize(ProtocolMessage message) {
        int payloadLength = message.getPayloadLength();
        return payloadLength >= MIN_PAYLOAD_SIZE && payloadLength <= MAX_PAYLOAD_SIZE;
    }
    
    


    private boolean validateChecksum(ProtocolMessage message) {
        int expectedChecksum = message.getChecksum();
        if (expectedChecksum == 0) {
            
            return true;
        }
        
        int calculatedChecksum = calculateChecksum(message);
        
        if (calculatedChecksum != expectedChecksum) {
            return false;
        }
        
        return true;
    }
    
    


    private int calculateChecksum(ProtocolMessage message) {
        crc32.reset();
        
        
        crc32.update(message.getMagic());
        
        
        ByteBuffer buffer = ByteBuffer.allocate(2);
        buffer.order(ByteOrder.BIG_ENDIAN);
        buffer.putShort((short) message.getProtocolVersion());
        crc32.update(buffer.array());
        
        
        if (message.getMessageType() != null) {
            crc32.update(message.getMessageType().getCode());
        }
        
        
        buffer = ByteBuffer.allocate(4);
        buffer.order(ByteOrder.BIG_ENDIAN);
        buffer.putInt(message.getSequenceNumber());
        crc32.update(buffer.array());
        
        
        buffer = ByteBuffer.allocate(4);
        buffer.order(ByteOrder.BIG_ENDIAN);
        buffer.putInt(message.getSessionId());
        crc32.update(buffer.array());
        
        
        buffer = ByteBuffer.allocate(4);
        buffer.order(ByteOrder.BIG_ENDIAN);
        buffer.putInt(message.getFlags());
        crc32.update(buffer.array());
        
        
        buffer = ByteBuffer.allocate(4);
        buffer.order(ByteOrder.BIG_ENDIAN);
        buffer.putInt(message.getPayloadLength());
        crc32.update(buffer.array());
        
        
        buffer = ByteBuffer.allocate(4);
        buffer.order(ByteOrder.BIG_ENDIAN);
        buffer.putInt(message.getTimestamp());
        crc32.update(buffer.array());
        
        
        crc32.update(message.getPayload());
        
        return (int) crc32.getValue();
    }
    
    



    private boolean validateMAC(ProtocolMessage message) {
        if (macKey == null || macKey.length == 0) {
            return true; 
        }
        
        int expectedMAC = message.getChecksum(); 
        if (expectedMAC == 0) {
            return true; 
        }
        
        int calculatedMAC = calculateMAC(message);
        return expectedMAC == calculatedMAC;
    }
    
    


    private int calculateMAC(ProtocolMessage message) {
        int mac = 0;
        
        
        byte[] data = message.serialize();
        for (int i = 0; i < data.length; i++) {
            mac ^= (data[i] & 0xFF) * (macKey[i % macKey.length] & 0xFF);
        }
        
        return mac & 0xFFFFFFFF;
    }
    
    


    private boolean validateTimestamp(ProtocolMessage message) {
        int timestamp = message.getTimestamp();
        int currentTime = (int) (System.currentTimeMillis() / 1000);
        
        
        int diff = Math.abs(timestamp - currentTime);
        return diff <= TIMESTAMP_TOLERANCE;
    }
    
    


    private boolean validateSequence(ProtocolMessage message) {
        int sequenceNumber = message.getSequenceNumber();
        
        return sequenceNumber >= 0;
    }
    
    


    private boolean validateMessageType(ProtocolMessage message) {
        return message.getMessageType() != null;
    }
    
    


    private boolean validateFragmentFields(ProtocolMessage message) {
        int fragmentIndex = message.getFragmentIndex();
        int totalFragments = message.getTotalFragments();
        int fragmentOffset = message.getFragmentOffset();
        int totalFragmentSize = message.getTotalFragmentSize();
        
        
        if (fragmentIndex < 0 || fragmentIndex >= totalFragments) {
            return false;
        }
        
        
        if (totalFragments <= 0 || totalFragments > 1000) {
            return false;
        }
        
        
        if (fragmentOffset < 0 || fragmentOffset >= totalFragmentSize) {
            return false;
        }
        
        
        if (totalFragmentSize <= 0 || totalFragmentSize > MAX_PAYLOAD_SIZE) {
            return false;
        }
        
        return true;
    }
    
    


    public ValidationResult validateBytes(byte[] data) {
        ValidationResult result = new ValidationResult();
        
        if (data == null || data.length < 35) {
            result.valid = false;
            result.errors.add("Data too short");
            return result;
        }
        
        
        if (validateMagicBytes) {
            if (data.length < 4 || 
                data[0] != ProtocolMessage.MAGIC_BYTES[0] ||
                data[1] != ProtocolMessage.MAGIC_BYTES[1] ||
                data[2] != ProtocolMessage.MAGIC_BYTES[2] ||
                data[3] != ProtocolMessage.MAGIC_BYTES[3]) {
                result.valid = false;
                result.errors.add("Invalid magic bytes");
                return result;
            }
        }
        
        
        try {
            ProtocolMessage message = ProtocolMessage.deserialize(data);
            return validate(message);
        } catch (Exception e) {
            result.valid = false;
            result.errors.add("Deserialization failed: " + e.getMessage());
            return result;
        }
    }
    
    


    public static class ValidationResult {
        public boolean valid;
        public final java.util.List<String> errors;
        
        public ValidationResult() {
            this.valid = true;
            this.errors = new java.util.ArrayList<>();
        }
    }
}
