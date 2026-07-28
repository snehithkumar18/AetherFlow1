package com.aetherflow;

import com.aetherflow.PacketAssembler;
import com.aetherflow.PacketAssembler.Fragment;





public class PacketAssemblerFuzzer {
    
    private static PacketAssembler packetAssembler;
    
    static {
        packetAssembler = new PacketAssembler();
    }
    
    


    public static void fuzzerTestOneInput(byte[] data) {
        try {
            if (packetAssembler == null || data == null || data.length < 8) {
                return;
            }
            
            
            int offset = 0;
            
            
            int sequenceNumber = ((data[offset] & 0xFF) << 24) |
                                ((data[offset + 1] & 0xFF) << 16) |
                                ((data[offset + 2] & 0xFF) << 8) |
                                (data[offset + 3] & 0xFF);
            offset += 4;
            
            
            int fragmentIndex = ((data[offset] & 0xFF) << 8) |
                               (data[offset + 1] & 0xFF);
            offset += 2;
            
            
            int totalFragments = ((data[offset] & 0xFF) << 8) |
                               (data[offset + 1] & 0xFF);
            offset += 2;
            
            
            int fragmentOffset = ((data[offset] & 0xFF) << 24) |
                                ((data[offset + 1] & 0xFF) << 16) |
                                ((data[offset + 2] & 0xFF) << 8) |
                                (data[offset + 3] & 0xFF);
            offset += 4;
            
            
            int totalSize = ((data[offset] & 0xFF) << 24) |
                           ((data[offset + 1] & 0xFF) << 16) |
                           ((data[offset + 2] & 0xFF) << 8) |
                           (data[offset + 3] & 0xFF);
            offset += 4;
            
            
            byte[] fragmentData = new byte[data.length - offset];
            System.arraycopy(data, offset, fragmentData, 0, fragmentData.length);
            
            
            Fragment fragment = new Fragment(
                sequenceNumber,
                fragmentIndex,
                totalFragments,
                fragmentOffset,
                totalSize,
                fragmentData
            );
            
            
            PacketAssembler.AssemblerResult result = packetAssembler.addFragment(fragment);
            
            if (result.success && result.data != null) {
                
                byte[] assembled = result.data;
                
                
                for (int i = 0; i < Math.min(assembled.length, 100); i++) {
                    byte b = assembled[i];
                }
            }
            
            
            PacketAssembler.ReassemblyContext context = packetAssembler.getContext(sequenceNumber);
            if (context != null) {
                context.getReceivedFragmentCount();
                context.getMissingFragmentCount();
                context.getMissingFragmentIndices();
                context.isComplete();
                context.getAge();
                context.getIdleTime();
            }
            
            
            packetAssembler.getStats();
            
            
            if (data.length >= 16) {
                byte[] testData = new byte[Math.min(data.length - 16, 1024)];
                System.arraycopy(data, 16, testData, 0, testData.length);
                packetAssembler.fragmentPacket(sequenceNumber + 1, testData, 256);
            }
            
            
            packetAssembler.cleanupExpiredContexts();
            
        } catch (Exception e) {
            
        }
    }
}
