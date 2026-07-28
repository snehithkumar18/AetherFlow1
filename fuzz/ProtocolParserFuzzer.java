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
            }
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
        }
    }
}
