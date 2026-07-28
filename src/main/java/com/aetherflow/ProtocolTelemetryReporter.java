package com.aetherflow;

import java.util.Map;




public class ProtocolTelemetryReporter {
    
    private final MetricsCollector metricsCollector;
    private long totalReportsSent;
    
    public ProtocolTelemetryReporter(MetricsCollector metricsCollector) {
        this.metricsCollector = metricsCollector;
        this.totalReportsSent = 0;
    }
    
    


    public synchronized String generateReport() {
        totalReportsSent++;
        MetricsCollector.MetricsSummary summary = metricsCollector.getSummary();
        
        StringBuilder report = new StringBuilder();
        report.append("--- AETHERFLOW TELEMETRY REPORT ---\n");
        report.append("Timestamp: ").append(System.currentTimeMillis()).append("\n");
        report.append("Report Sequence: ").append(totalReportsSent).append("\n");
        report.append("Metrics collected:\n");
        
        report.append("  totalMetrics: ").append(summary.totalMetrics).append("\n");
        report.append("  totalHistograms: ").append(summary.totalHistograms).append("\n");
        report.append("  totalMetricCount: ").append(summary.totalMetricCount).append("\n");
        report.append("  totalHistogramObservations: ").append(summary.totalHistogramObservations).append("\n");
        
        report.append("------------------------------------\n");
        return report.toString();
    }
    
    public long getTotalReportsSent() {
        return totalReportsSent;
    }
}
