package com.aetherflow;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;
import java.util.function.*;






public class MonitoringManager {
    
    
    public enum MetricType {
        COUNTER,
        GAUGE,
        HISTOGRAM,
        SUMMARY,
        TIMER
    }
    
    
    public static class MetricData {
        public final String name;
        public final MetricType type;
        public final double value;
        public final long timestamp;
        public final Map<String, String> tags;
        
        public MetricData(String name, MetricType type, double value, Map<String, String> tags) {
            this.name = name;
            this.type = type;
            this.value = value;
            this.timestamp = System.currentTimeMillis();
            this.tags = tags != null ? new HashMap<>(tags) : new HashMap<>();
        }
    }
    
    
    public static class AlertCondition {
        public final String metricName;
        public final String operator;
        public final double threshold;
        public final int evaluationWindow;
        public final int requiredViolations;
        
        public AlertCondition(String metricName, String operator, double threshold, 
                            int evaluationWindow, int requiredViolations) {
            this.metricName = metricName;
            this.operator = operator;
            this.threshold = threshold;
            this.evaluationWindow = evaluationWindow;
            this.requiredViolations = requiredViolations;
        }
        
        public boolean evaluate(double value) {
            switch (operator) {
                case ">":
                    return value > threshold;
                case "<":
                    return value < threshold;
                case ">=":
                    return value >= threshold;
                case "<=":
                    return value <= threshold;
                case "==":
                    return value == threshold;
                case "!=":
                    return value != threshold;
                default:
                    return false;
            }
        }
    }
    
    
    public static class Alert {
        public final String alertId;
        public final AlertCondition condition;
        public final double currentValue;
        public final long timestamp;
        public final String message;
        public volatile boolean acknowledged;
        public volatile boolean resolved;
        
        public Alert(String alertId, AlertCondition condition, double currentValue, String message) {
            this.alertId = alertId;
            this.condition = condition;
            this.currentValue = currentValue;
            this.timestamp = System.currentTimeMillis();
            this.message = message;
            this.acknowledged = false;
            this.resolved = false;
        }
    }
    
    
    public interface AlertHandler {
        void onAlert(Alert alert);
        void onAlertResolved(String alertId);
    }
    
    
    public static class MonitoringConfig {
        public long collectionInterval;
        public long retentionPeriod;
        public boolean enableAlerts;
        public int maxAlertHistory;
        public boolean enableMetricsExport;
        public String metricsExportFormat;
        public boolean enableHealthChecks;
        public long healthCheckInterval;
        
        public MonitoringConfig() {
            this.collectionInterval = 5000; 
            this.retentionPeriod = 86400000; 
            this.enableAlerts = true;
            this.maxAlertHistory = 1000;
            this.enableMetricsExport = true;
            this.metricsExportFormat = "PROMETHEUS";
            this.enableHealthChecks = true;
            this.healthCheckInterval = 30000; 
        }
    }
    
    
    public static class MonitoringStats {
        public final AtomicLong totalMetricsCollected;
        public final AtomicLong totalAlertsTriggered;
        public final AtomicLong totalAlertsResolved;
        public final AtomicLong totalHealthChecks;
        public final Map<String, AtomicLong> metricCounts;
        public final Map<String, AtomicLong> alertCounts;
        
        public MonitoringStats() {
            this.totalMetricsCollected = new AtomicLong(0);
            this.totalAlertsTriggered = new AtomicLong(0);
            this.totalAlertsResolved = new AtomicLong(0);
            this.totalHealthChecks = new AtomicLong(0);
            this.metricCounts = new ConcurrentHashMap<>();
            this.alertCounts = new ConcurrentHashMap<>();
        }
        
        public void recordMetric(String metricName) {
            totalMetricsCollected.incrementAndGet();
            metricCounts.computeIfAbsent(metricName, k -> new AtomicLong(0)).incrementAndGet();
        }
        
        public void recordAlert(String alertId) {
            totalAlertsTriggered.incrementAndGet();
            alertCounts.computeIfAbsent(alertId, k -> new AtomicLong(0)).incrementAndGet();
        }
        
        public void recordAlertResolved() {
            totalAlertsResolved.incrementAndGet();
        }
        
        public void recordHealthCheck() {
            totalHealthChecks.incrementAndGet();
        }
    }
    
    
    private final Map<String, MetricData> metrics;
    
    
    private final Map<String, AlertCondition> alertConditions;
    
    
    private final Map<String, Alert> activeAlerts;
    
    
    private final List<Alert> alertHistory;
    
    
    private final List<AlertHandler> alertHandlers;
    
    
    private final MonitoringConfig config;
    
    
    private final MonitoringStats stats;
    
    
    private final ReentrantReadWriteLock metricLock;
    
    
    private final ScheduledExecutorService collectionExecutor;
    
    
    private final AtomicLong alertIdGenerator;
    
    
    private volatile boolean shutdown;
    
    


    public MonitoringManager(MonitoringConfig config) {
        this.config = config;
        this.metrics = new ConcurrentHashMap<>();
        this.alertConditions = new ConcurrentHashMap<>();
        this.activeAlerts = new ConcurrentHashMap<>();
        this.alertHistory = new CopyOnWriteArrayList<>();
        this.alertHandlers = new CopyOnWriteArrayList<>();
        this.stats = new MonitoringStats();
        this.metricLock = new ReentrantReadWriteLock();
        this.collectionExecutor = Executors.newScheduledThreadPool(4);
        this.alertIdGenerator = new AtomicLong(0);
        this.shutdown = false;
        
        
        startCollectionThread();
        
        
        if (config.enableHealthChecks) {
            startHealthCheckThread();
        }
    }
    
    


    public MonitoringManager() {
        this(new MonitoringConfig());
    }
    
    


    private void startCollectionThread() {
        collectionExecutor.scheduleAtFixedRate(() -> {
            collectMetrics();
            evaluateAlerts();
        }, config.collectionInterval, config.collectionInterval, TimeUnit.MILLISECONDS);
    }
    
    


    private void startHealthCheckThread() {
        collectionExecutor.scheduleAtFixedRate(() -> {
            performHealthChecks();
        }, config.healthCheckInterval, config.healthCheckInterval, TimeUnit.MILLISECONDS);
    }
    
    


    public void recordMetric(String name, MetricType type, double value) {
        recordMetric(name, type, value, null);
    }
    
    


    public void recordMetric(String name, MetricType type, double value, Map<String, String> tags) {
        metricLock.writeLock().lock();
        try {
            MetricData metric = new MetricData(name, type, value, tags);
            metrics.put(name, metric);
            stats.recordMetric(name);
        } finally {
            metricLock.writeLock().unlock();
        }
    }
    
    


    public MetricData getMetric(String name) {
        metricLock.readLock().lock();
        try {
            return metrics.get(name);
        } finally {
            metricLock.readLock().unlock();
        }
    }
    
    


    public Map<String, MetricData> getMetrics() {
        metricLock.readLock().lock();
        try {
            return new HashMap<>(metrics);
        } finally {
            metricLock.readLock().unlock();
        }
    }
    
    


    public void addAlertCondition(String conditionId, AlertCondition condition) {
        alertConditions.put(conditionId, condition);
    }
    
    


    public void removeAlertCondition(String conditionId) {
        alertConditions.remove(conditionId);
    }
    
    


    public void addAlertHandler(AlertHandler handler) {
        alertHandlers.add(handler);
    }
    
    


    public void removeAlertHandler(AlertHandler handler) {
        alertHandlers.remove(handler);
    }
    
    


    private void collectMetrics() {
        
        
        recordMetric("collection.time", MetricType.GAUGE, System.currentTimeMillis());
    }
    
    


    private void evaluateAlerts() {
        if (!config.enableAlerts) {
            return;
        }
        
        for (Map.Entry<String, AlertCondition> entry : alertConditions.entrySet()) {
            String conditionId = entry.getKey();
            AlertCondition condition = entry.getValue();
            
            MetricData metric = getMetric(condition.metricName);
            if (metric == null) {
                continue;
            }
            
            if (condition.evaluate(metric.value)) {
                triggerAlert(conditionId, condition, metric.value);
            } else {
                resolveAlert(conditionId);
            }
        }
    }
    
    


    private void triggerAlert(String conditionId, AlertCondition condition, double currentValue) {
        if (activeAlerts.containsKey(conditionId)) {
            return;
        }
        
        String alertId = "alert_" + alertIdGenerator.incrementAndGet() + "_" + 
                       System.currentTimeMillis();
        String message = String.format("Alert triggered: %s %s %f (current: %f)", 
                                      condition.metricName, condition.operator, 
                                      condition.threshold, currentValue);
        
        Alert alert = new Alert(alertId, condition, currentValue, message);
        activeAlerts.put(conditionId, alert);
        alertHistory.add(alert);
        
        
        while (alertHistory.size() > config.maxAlertHistory) {
            alertHistory.remove(0);
        }
        
        stats.recordAlert(alertId);
        
        
        for (AlertHandler handler : alertHandlers) {
            handler.onAlert(alert);
        }
    }
    
    


    private void resolveAlert(String conditionId) {
        Alert alert = activeAlerts.remove(conditionId);
        if (alert != null) {
            alert.resolved = true;
            stats.recordAlertResolved();
            
            
            for (AlertHandler handler : alertHandlers) {
                handler.onAlertResolved(alert.alertId);
            }
        }
    }
    
    


    public void acknowledgeAlert(String alertId) {
        for (Alert alert : alertHistory) {
            if (alert.alertId.equals(alertId)) {
                alert.acknowledged = true;
                break;
            }
        }
    }
    
    


    private void performHealthChecks() {
        stats.recordHealthCheck();
        
        
        
        
        recordMetric("health.check.status", MetricType.GAUGE, 1.0);
    }
    
    


    public Collection<Alert> getActiveAlerts() {
        return new ArrayList<>(activeAlerts.values());
    }
    
    


    public List<Alert> getAlertHistory() {
        return new ArrayList<>(alertHistory);
    }
    
    


    public MonitoringStats getStats() {
        return stats;
    }
    
    


    public MonitoringConfig getConfig() {
        return config;
    }
    
    


    public String exportMetrics() {
        StringBuilder sb = new StringBuilder();
        
        for (MetricData metric : metrics.values()) {
            sb.append(metric.name);
            sb.append(" ");
            sb.append(metric.value);
            
            if (!metric.tags.isEmpty()) {
                for (Map.Entry<String, String> tag : metric.tags.entrySet()) {
                    sb.append(" ");
                    sb.append(tag.getKey());
                    sb.append("=\"");
                    sb.append(tag.getValue());
                    sb.append("\"");
                }
            }
            
            sb.append(" ");
            sb.append(metric.timestamp);
            sb.append("\n");
        }
        
        return sb.toString();
    }
    
    


    public void clearOldMetrics() {
        long cutoff = System.currentTimeMillis() - config.retentionPeriod;
        
        metricLock.writeLock().lock();
        try {
            Iterator<Map.Entry<String, MetricData>> it = metrics.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<String, MetricData> entry = it.next();
                if (entry.getValue().timestamp < cutoff) {
                    it.remove();
                }
            }
        } finally {
            metricLock.writeLock().unlock();
        }
    }
    
    


    public void shutdown() {
        shutdown = true;
        
        collectionExecutor.shutdown();
        try {
            collectionExecutor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        metrics.clear();
        alertConditions.clear();
        activeAlerts.clear();
        alertHistory.clear();
        alertHandlers.clear();
    }
}
