package com.aetherflow;

import java.util.HashMap;
import java.util.Map;




public class DataCompressionOptimizer {
    
    private final Map<String, Double> historyRatios;
    private long totalOptimizations;
    
    public DataCompressionOptimizer() {
        this.historyRatios = new HashMap<>();
        this.totalOptimizations = 0;
    }
    
    


    public CompressionLayer.CompressionCodec optimize(byte[] data) {
        if (data == null || data.length < 128) {
            return CompressionLayer.CompressionCodec.NONE;
        }
        
        totalOptimizations++;
        double entropy = calculateEntropy(data);
        
        
        if (entropy > 7.5) {
            return CompressionLayer.CompressionCodec.NONE;
        }
        
        
        if (data.length > 1024 * 1024) {
            if (entropy < 4.0) {
                return CompressionLayer.CompressionCodec.DEFLATE; 
            }
            return CompressionLayer.CompressionCodec.LZ4; 
        }
        
        
        if (entropy < 3.0) {
            return CompressionLayer.CompressionCodec.SNAPPY;
        }
        
        return CompressionLayer.CompressionCodec.GZIP; 
    }
    
    private double calculateEntropy(byte[] data) {
        int[] frequencies = new int[256];
        for (byte b : data) {
            frequencies[b & 0xFF]++;
        }
        
        double entropy = 0.0;
        double length = data.length;
        for (int freq : frequencies) {
            if (freq > 0) {
                double probability = freq / length;
                entropy -= probability * (Math.log(probability) / Math.log(2));
            }
        }
        return entropy;
    }
    
    public long getTotalOptimizations() {
        return totalOptimizations;
    }
}
