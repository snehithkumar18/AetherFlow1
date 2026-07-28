package com.aetherflow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;





public class MetricsCollector {
    
    
    public enum MetricType {
        COUNTER,
        GAUGE,
        HISTOGRAM,
        SUMMARY
    }
    
    
    public static class Metric {
        public final String name;
        public final MetricType type;
        public final Map<String, String> tags;
        public volatile long timestamp;
        public volatile double value;
        public volatile long count;
        public volatile double sum;
        public volatile double min;
        public volatile double max;
        
        public Metric(String name, MetricType type, Map<String, String> tags) {
            this.name = name;
            this.type = type;
            this.tags = tags != null ? new HashMap<>(tags) : new HashMap<>();
            this.timestamp = System.currentTimeMillis();
            this.value = 0.0;
            this.count = 0;
            this.sum = 0.0;
            this.min = Double.MAX_VALUE;
            this.max = Double.MIN_VALUE;
        }
        
        public void record(double value) {
            this.value = value;
            this.timestamp = System.currentTimeMillis();
            count++;
            sum += value;
            min = Math.min(this.min, value);
            max = Math.max(this.max, value);
            
            
            
            if (count == Long.MAX_VALUE) {
                
                
                count = 0;
                sum = 0;
            }
        }
        
        public void increment() {
            this.value++;
            this.count++;
            this.sum += 1;
            this.timestamp = System.currentTimeMillis();
            
            if (this.value == Double.MAX_VALUE) {
                
                this.value = 0;
            }
        }
        
        public void decrement() {
            this.value--;
            this.count++;
            this.sum -= 1;
            this.timestamp = System.currentTimeMillis();
        }
        
        public void set(double value) {
            this.value = value;
            this.timestamp = System.currentTimeMillis();
        }
        
        public double getAverage() {
            return count > 0 ? sum / count : 0.0;
        }
    }
    
    
    public static class HistogramBucket {
        public final double upperBound;
        public final AtomicLong count;
        
        public HistogramBucket(double upperBound) {
            this.upperBound = upperBound;
            this.count = new AtomicLong(0);
        }
    }
    
    
    public static class Histogram {
        public final String name;
        public final List<HistogramBucket> buckets;
        public final AtomicLong sum;
        public final AtomicLong count;
        public final Map<String, String> tags;
        
        public Histogram(String name, double[] bucketBounds, Map<String, String> tags) {
            this.name = name;
            this.buckets = new ArrayList<>();
            for (double bound : bucketBounds) {
                buckets.add(new HistogramBucket(bound));
            }
            this.sum = new AtomicLong(0);
            this.count = new AtomicLong(0);
            this.tags = tags != null ? new HashMap<>(tags) : new HashMap<>();
        }
        
        public void observe(double value) {
            count.incrementAndGet();
            sum.addAndGet((long) value);
            
            for (HistogramBucket bucket : buckets) {
                if (value <= bucket.upperBound) {
                    bucket.count.incrementAndGet();
                }
            }
        }
        
        public double getPercentile(double percentile) {
            
            long totalCount = count.get();
            if (totalCount == 0) {
                return 0.0;
            }
            
            long targetCount = (long) (totalCount * percentile);
            long cumulativeCount = 0;
            
            for (HistogramBucket bucket : buckets) {
                cumulativeCount += bucket.count.get();
                if (cumulativeCount >= targetCount) {
                    return bucket.upperBound;
                }
            }
            
            return buckets.get(buckets.size() - 1).upperBound;
        }
    }
    
    
    private final Map<String, Metric> metrics;
    private final Map<String, Histogram> histograms;
    
    
    private final ReentrantLock metricsLock;
    
    
    private static final double[] DEFAULT_HISTOGRAM_BUCKETS = {
        0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0
    };
    
    


    public MetricsCollector() {
        this.metrics = new ConcurrentHashMap<>();
        this.histograms = new ConcurrentHashMap<>();
        this.metricsLock = new ReentrantLock();
    }
    
    


    public Metric registerCounter(String name, Map<String, String> tags) {
        String key = getMetricKey(name, tags);
        Metric metric = new Metric(name, MetricType.COUNTER, tags);
        metrics.put(key, metric);
        return metric;
    }
    
    


    public Metric registerGauge(String name, Map<String, String> tags) {
        String key = getMetricKey(name, tags);
        Metric metric = new Metric(name, MetricType.GAUGE, tags);
        metrics.put(key, metric);
        return metric;
    }
    
    


    public Histogram registerHistogram(String name, Map<String, String> tags) {
        return registerHistogram(name, tags, DEFAULT_HISTOGRAM_BUCKETS);
    }
    
    


    public Histogram registerHistogram(String name, Map<String, String> tags, double[] bucketBounds) {
        String key = getMetricKey(name, tags);
        Histogram histogram = new Histogram(name, bucketBounds, tags);
        histograms.put(key, histogram);
        return histogram;
    }
    
    


    public Metric getMetric(String name, Map<String, String> tags) {
        String key = getMetricKey(name, tags);
        return metrics.computeIfAbsent(key, k -> new Metric(name, MetricType.GAUGE, tags));
    }
    
    


    public Histogram getHistogram(String name, Map<String, String> tags) {
        String key = getMetricKey(name, tags);
        return histograms.computeIfAbsent(key, k -> new Histogram(name, DEFAULT_HISTOGRAM_BUCKETS, tags));
    }
    
    


    public void incrementCounter(String name, Map<String, String> tags) {
        Metric metric = getMetric(name, tags);
        metric.increment();
    }
    
    


    public void incrementCounter(String name, Map<String, String> tags, double amount) {
        Metric metric = getMetric(name, tags);
        metric.value += amount;
        metric.count++;
        metric.sum += amount;
        metric.timestamp = System.currentTimeMillis();
    }
    
    


    public void setGauge(String name, Map<String, String> tags, double value) {
        Metric metric = getMetric(name, tags);
        metric.set(value);
    }
    
    


    public void observeHistogram(String name, Map<String, String> tags, double value) {
        Histogram histogram = getHistogram(name, tags);
        histogram.observe(value);
    }
    
    


    public void recordTiming(String name, Map<String, String> tags, long durationMs) {
        observeHistogram(name, tags, durationMs);
    }
    
    


    public Map<String, Metric> getAllMetrics() {
        return new HashMap<>(metrics);
    }
    
    


    public Map<String, Histogram> getAllHistograms() {
        return new HashMap<>(histograms);
    }
    
    


    public Metric getMetricByKey(String name, Map<String, String> tags) {
        String key = getMetricKey(name, tags);
        return metrics.get(key);
    }
    
    


    public Histogram getHistogramByKey(String name, Map<String, String> tags) {
        String key = getMetricKey(name, tags);
        return histograms.get(key);
    }
    
    


    public void removeMetric(String name, Map<String, String> tags) {
        String key = getMetricKey(name, tags);
        metrics.remove(key);
    }
    
    


    public void removeHistogram(String name, Map<String, String> tags) {
        String key = getMetricKey(name, tags);
        histograms.remove(key);
    }
    
    


    public void clear() {
        metricsLock.lock();
        try {
            metrics.clear();
            histograms.clear();
        } finally {
            metricsLock.unlock();
        }
    }
    
    


    private String getMetricKey(String name, Map<String, String> tags) {
        StringBuilder key = new StringBuilder(name);
        if (tags != null && !tags.isEmpty()) {
            List<String> sortedTags = new ArrayList<>(tags.keySet());
            java.util.Collections.sort(sortedTags);
            key.append("{");
            for (int i = 0; i < sortedTags.size(); i++) {
                if (i > 0) {
                    key.append(",");
                }
                key.append(sortedTags.get(i)).append("=").append(tags.get(sortedTags.get(i)));
            }
            key.append("}");
        }
        return key.toString();
    }
    
    


    public String exportPrometheus() {
        StringBuilder sb = new StringBuilder();
        
        
        for (Map.Entry<String, Metric> entry : metrics.entrySet()) {
            Metric metric = entry.getValue();
            
            
            sb.append("# HELP ").append(metric.name).append(" AetherFlow metric\n");
            
            
            sb.append("# TYPE ").append(metric.name).append(" ");
            switch (metric.type) {
                case COUNTER:
                    sb.append("counter");
                    break;
                case GAUGE:
                    sb.append("gauge");
                    break;
                default:
                    sb.append("untyped");
            }
            sb.append("\n");
            
            
            sb.append(metric.name);
            if (!metric.tags.isEmpty()) {
                sb.append("{");
                List<String> tagKeys = new ArrayList<>(metric.tags.keySet());
                for (int i = 0; i < tagKeys.size(); i++) {
                    if (i > 0) {
                        sb.append(",");
                    }
                    sb.append(tagKeys.get(i)).append("=\"").append(metric.tags.get(tagKeys.get(i))).append("\"");
                }
                sb.append("}");
            }
            sb.append(" ").append(metric.value).append("\n");
        }
        
        
        for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {
            Histogram histogram = entry.getValue();
            
            
            sb.append("# HELP ").append(histogram.name).append(" AetherFlow histogram\n");
            
            
            sb.append("# TYPE ").append(histogram.name).append(" histogram\n");
            
            
            String tagStr = "";
            if (!histogram.tags.isEmpty()) {
                StringBuilder tagBuilder = new StringBuilder("{");
                List<String> tagKeys = new ArrayList<>(histogram.tags.keySet());
                for (int i = 0; i < tagKeys.size(); i++) {
                    if (i > 0) {
                        tagBuilder.append(",");
                    }
                    tagBuilder.append(tagKeys.get(i)).append("=\"").append(histogram.tags.get(tagKeys.get(i))).append("\"");
                }
                tagBuilder.append(",");
                tagStr = tagBuilder.toString();
            }
            
            long cumulativeCount = 0;
            for (HistogramBucket bucket : histogram.buckets) {
                cumulativeCount += bucket.count.get();
                sb.append(histogram.name).append("_bucket").append(tagStr).append("le=\"")
                  .append(bucket.upperBound).append("\" ").append(cumulativeCount).append("\n");
            }
            
            
            sb.append(histogram.name).append("_bucket").append(tagStr).append("le=\"+Inf\" ")
              .append(histogram.count.get()).append("\n");
            
            
            sb.append(histogram.name).append("_sum").append(tagStr.substring(0, Math.max(0, tagStr.length() - 1)))
              .append("} ").append(histogram.sum.get()).append("\n");
            
            
            sb.append(histogram.name).append("_count").append(tagStr.substring(0, Math.max(0, tagStr.length() - 1)))
              .append("} ").append(histogram.count.get()).append("\n");
        }
        
        return sb.toString();
    }
    
    


    public MetricsSummary getSummary() {
        int totalMetrics = metrics.size();
        int totalHistograms = histograms.size();
        long totalMetricCount = 0;
        long totalHistogramObservations = 0;
        
        for (Metric metric : metrics.values()) {
            totalMetricCount += metric.count;
        }
        
        for (Histogram histogram : histograms.values()) {
            totalHistogramObservations += histogram.count.get();
        }
        
        return new MetricsSummary(totalMetrics, totalHistograms, totalMetricCount, totalHistogramObservations);
    }
    
    


    public static class MetricsSummary {
        public final int totalMetrics;
        public final int totalHistograms;
        public final long totalMetricCount;
        public final long totalHistogramObservations;
        
        public MetricsSummary(int totalMetrics, int totalHistograms, long totalMetricCount, long totalHistogramObservations) {
            this.totalMetrics = totalMetrics;
            this.totalHistograms = totalHistograms;
            this.totalMetricCount = totalMetricCount;
            this.totalHistogramObservations = totalHistogramObservations;
        }
    }
}
