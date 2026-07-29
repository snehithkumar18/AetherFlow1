package com.aetherflow;

import com.aetherflow.ProtocolMessage;

public class ProtocolParserFuzzer {
    
    public static void fuzzerTestOneInput(byte[] data) {
        if (data == null || data.length < 35) {
            return;
        }

        try {
            ProtocolMessage message = ProtocolMessage.deserialize(data);
            if (message != null) {
                message.validate();
                byte[] serialized = message.serialize();
                ProtocolMessage message2 = ProtocolMessage.deserialize(serialized);
                
                ProtocolValidator validator = new ProtocolValidator(ProtocolValidator.ValidationLevel.STRICT);
                ProtocolValidator.ValidationResult result = validator.validate(message);
                
                if (message.getMessageType() == ProtocolMessage.MessageType.DATA) {
                    BinaryCodec.decode(message.getPayload());
                }
                
                if (message.getMessageType() == ProtocolMessage.MessageType.COMPRESSED_DATA) {
                    CompressionLayer layer = new CompressionLayer();
                    layer.decompress(message.getPayload(), CompressionLayer.CompressionCodec.GZIP);
                }
                
                if (message.getMessageType() == ProtocolMessage.MessageType.ENCRYPTED_DATA) {
                    try {
                        EncryptionLayer encLayer = new EncryptionLayer();
                        String sessionId = "sess_" + (message.getSessionId() % 100);
                        encLayer.generateSessionKey(sessionId);
                        encLayer.encrypt(message.getPayload(), sessionId);
                        encLayer.decrypt(message.getPayload(), sessionId);
                    } catch (Exception e) {
                    }
                }
                
                if (data.length > 50) {
                    PacketAssembler assembler = new PacketAssembler();
                    PacketAssembler.Fragment frag = new PacketAssembler.Fragment(
                        message.getSequenceNumber(),
                        0,
                        1,
                        0,
                        message.getPayloadLength(),
                        message.getPayload()
                    );
                    assembler.addFragment(frag);
                }
            }
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
        }
    }
}
