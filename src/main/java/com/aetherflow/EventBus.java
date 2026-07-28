package com.aetherflow;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;
import java.util.function.*;






public class EventBus {
    
    
    public static class Event {
        public final String eventType;
        public final Map<String, Object> payload;
        public final long timestamp;
        public final String eventId;
        public final Map<String, String> headers;
        
        public Event(String eventType, Map<String, Object> payload, Map<String, String> headers) {
            this.eventType = eventType;
            this.payload = payload != null ? new HashMap<>(payload) : new HashMap<>();
            this.headers = headers != null ? new HashMap<>(headers) : new HashMap<>();
            this.timestamp = System.currentTimeMillis();
            this.eventId = UUID.randomUUID().toString();
        }
        
        public Event(String eventType, Map<String, Object> payload) {
            this(eventType, payload, null);
        }
        
        public <T> T getPayload(String key, Class<T> type) {
            Object value = payload.get(key);
            if (value != null && type.isInstance(value)) {
                return type.cast(value);
            }
            return null;
        }
        
        public void setPayload(String key, Object value) {
            payload.put(key, value);
        }
        
        public String getHeader(String key) {
            return headers.get(key);
        }
        
        public void setHeader(String key, String value) {
            headers.put(key, value);
        }
    }
    
    
    public interface EventHandler {
        void handleEvent(Event event);
        void onError(Throwable error, Event event);
    }
    
    
    public interface EventFilter {
        boolean matches(Event event);
    }
    
    
    public static class Subscription {
        public final String subscriptionId;
        public final String eventType;
        public final EventHandler handler;
        public final EventFilter filter;
        public volatile boolean active;
        public final long creationTime;
        public volatile int eventCount;
        public final Map<String, Object> metadata;
        public final ReentrantLock subscriptionLock;
        
        public Subscription(String subscriptionId, String eventType, EventHandler handler, 
                           EventFilter filter, Map<String, Object> metadata) {
            this.subscriptionId = subscriptionId;
            this.eventType = eventType;
            this.handler = handler;
            this.filter = filter;
            this.active = true;
            this.creationTime = System.currentTimeMillis();
            this.eventCount = 0;
            this.metadata = metadata != null ? new HashMap<>(metadata) : new HashMap<>();
            this.subscriptionLock = new ReentrantLock();
        }
        
        public long getAge() {
            return System.currentTimeMillis() - creationTime;
        }
        
        public void incrementEventCount() {
            eventCount++;
        }
    }
    
    
    public static class EventBusConfig {
        public int maxEventQueueSize;
        public int workerThreads;
        public long eventTimeout;
        public boolean enableAsyncDelivery;
        public boolean enableEventReplay;
        public int maxReplayEvents;
        public boolean enableMetrics;
        public boolean enableDeadLetterQueue;
        
        public EventBusConfig() {
            this.maxEventQueueSize = 10000;
            this.workerThreads = Runtime.getRuntime().availableProcessors();
            this.eventTimeout = 30000;
            this.enableAsyncDelivery = true;
            this.enableEventReplay = false;
            this.maxReplayEvents = 1000;
            this.enableMetrics = true;
            this.enableDeadLetterQueue = true;
        }
    }
    
    
    public static class EventBusStats {
        public final AtomicLong totalEventsPublished;
        public final AtomicLong totalEventsDelivered;
        public final AtomicLong totalEventsDropped;
        public final AtomicLong totalEventsFailed;
        public final AtomicLong totalSubscriptions;
        public final AtomicLong activeSubscriptions;
        public final Map<String, AtomicLong> eventTypeCounts;
        public final Map<String, AtomicLong> subscriptionCounts;
        public final Map<String, AtomicLong> errorCounts;
        
        public EventBusStats() {
            this.totalEventsPublished = new AtomicLong(0);
            this.totalEventsDelivered = new AtomicLong(0);
            this.totalEventsDropped = new AtomicLong(0);
            this.totalEventsFailed = new AtomicLong(0);
            this.totalSubscriptions = new AtomicLong(0);
            this.activeSubscriptions = new AtomicLong(0);
            this.eventTypeCounts = new ConcurrentHashMap<>();
            this.subscriptionCounts = new ConcurrentHashMap<>();
            this.errorCounts = new ConcurrentHashMap<>();
        }
        
        public void recordEventPublished(String eventType) {
            totalEventsPublished.incrementAndGet();
            eventTypeCounts.computeIfAbsent(eventType, k -> new AtomicLong(0)).incrementAndGet();
        }
        
        public void recordEventDelivered(String subscriptionId) {
            totalEventsDelivered.incrementAndGet();
            subscriptionCounts.computeIfAbsent(subscriptionId, k -> new AtomicLong(0)).incrementAndGet();
        }
        
        public void recordEventDropped() {
            totalEventsDropped.incrementAndGet();
        }
        
        public void recordEventFailed(String errorType) {
            totalEventsFailed.incrementAndGet();
            errorCounts.computeIfAbsent(errorType, k -> new AtomicLong(0)).incrementAndGet();
        }
    }
    
    
    private final EventBusConfig config;
    
    
    private final Map<String, List<Subscription>> subscriptions;
    
    
    private final Map<String, Subscription> allSubscriptions;
    
    
    private final BlockingQueue<Event> eventQueue;
    
    
    private final ExecutorService workerExecutor;
    
    
    private final BlockingQueue<Event> deadLetterQueue;
    
    
    private final List<Event> replayBuffer;
    
    
    private final EventBusStats stats;
    
    
    private final ReentrantLock subscriptionLock;
    
    
    private final AtomicLong subscriptionIdGenerator;
    
    
    private volatile boolean shutdown;
    
    


    public EventBus(EventBusConfig config) {
        this.config = config;
        this.subscriptions = new ConcurrentHashMap<>();
        this.allSubscriptions = new ConcurrentHashMap<>();
        this.eventQueue = new LinkedBlockingQueue<>(config.maxEventQueueSize);
        this.workerExecutor = Executors.newFixedThreadPool(config.workerThreads);
        this.deadLetterQueue = new LinkedBlockingQueue<>();
        this.replayBuffer = new ArrayList<>();
        this.stats = new EventBusStats();
        this.subscriptionLock = new ReentrantLock();
        this.subscriptionIdGenerator = new AtomicLong(0);
        this.shutdown = false;
        
        
        startWorkers();
    }
    
    


    public EventBus() {
        this(new EventBusConfig());
    }
    
    


    private void startWorkers() {
        for (int i = 0; i < config.workerThreads; i++) {
            workerExecutor.submit(() -> {
                while (!shutdown) {
                    try {
                        Event event = eventQueue.poll(config.eventTimeout, TimeUnit.MILLISECONDS);
                        if (event != null) {
                            deliverEvent(event);
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    } catch (Exception e) {
                        stats.totalEventsFailed.incrementAndGet();
                        stats.recordEventFailed(e.getClass().getSimpleName());
                        
                        if (stats.totalEventsFailed.get() > 10000) {
                            stats.totalEventsFailed.set(0);
                        }
                    }
                }
            });
        }
    }
    
    


    public String subscribe(String eventType, EventHandler handler) {
        return subscribe(eventType, handler, null, null);
    }
    
    


    public String subscribe(String eventType, EventHandler handler, EventFilter filter) {
        return subscribe(eventType, handler, filter, null);
    }
    
    


    public String subscribe(String eventType, EventHandler handler, EventFilter filter, 
                           Map<String, Object> metadata) {
        String subscriptionId = "sub_" + subscriptionIdGenerator.incrementAndGet() + "_" + 
                               System.currentTimeMillis();
        
        Subscription subscription = new Subscription(subscriptionId, eventType, handler, 
                                                   filter, metadata);
        
        subscriptionLock.lock();
        try {
            subscriptions.computeIfAbsent(eventType, k -> new CopyOnWriteArrayList())
                        .add(subscription);
            allSubscriptions.put(subscriptionId, subscription);
            
            stats.totalSubscriptions.incrementAndGet();
            stats.activeSubscriptions.incrementAndGet();
            
            return subscriptionId;
        } finally {
            subscriptionLock.unlock();
        }
    }
    
    


    public void unsubscribe(String subscriptionId) {
        subscriptionLock.lock();
        try {
            Subscription subscription = allSubscriptions.remove(subscriptionId);
            if (subscription != null) {
                subscription.active = false;
                
                List<Subscription> typeSubscriptions = subscriptions.get(subscription.eventType);
                if (typeSubscriptions != null) {
                    typeSubscriptions.remove(subscription);
                }
                
                stats.activeSubscriptions.decrementAndGet();
            }
        } finally {
            subscriptionLock.unlock();
        }
    }
    
    


    public boolean publish(Event event) {
        stats.recordEventPublished(event.eventType);
        
        
        if (config.enableEventReplay) {
            synchronized (replayBuffer) {
                replayBuffer.add(event);
                if (replayBuffer.size() > config.maxReplayEvents) {
                    replayBuffer.remove(0);
                }
            }
        }
        
        if (config.enableAsyncDelivery) {
            try {
                boolean added = eventQueue.offer(event, config.eventTimeout, TimeUnit.MILLISECONDS);
                if (!added) {
                    stats.recordEventDropped();
                    if (config.enableDeadLetterQueue) {
                        deadLetterQueue.offer(event);
                    }
                }
                return added;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                stats.recordEventDropped();
                return false;
            }
        } else {
            return deliverEvent(event);
        }
    }
    
    


    private boolean deliverEvent(Event event) {
        subscriptionLock.lock();
        try {
            List<Subscription> eventSubscriptions = subscriptions.get(event.eventType);
            if (eventSubscriptions == null || eventSubscriptions.isEmpty()) {
                return false;
            }
            
            boolean delivered = false;
            
            for (Subscription subscription : eventSubscriptions) {
                if (!subscription.active) {
                    continue;
                }
                
                
                if (subscription.filter != null && !subscription.filter.matches(event)) {
                    continue;
                }
                
                
                
                subscription.subscriptionLock.lock();
                try {
                    if (subscription.active) {
                        try {
                            subscription.handler.handleEvent(event);
                            subscription.incrementEventCount();
                            stats.recordEventDelivered(subscription.subscriptionId);
                            delivered = true;
                        } catch (Exception e) {
                            stats.totalEventsFailed.incrementAndGet();
                            stats.recordEventFailed(e.getClass().getSimpleName());
                            subscription.handler.onError(e, event);
                        }
                    }
                } finally {
                    subscription.subscriptionLock.unlock();
                }
            }
            
            return delivered;
        } finally {
            subscriptionLock.unlock();
        }
    }
    
    


    public Subscription getSubscription(String subscriptionId) {
        return allSubscriptions.get(subscriptionId);
    }
    
    


    public Collection<Subscription> getSubscriptions() {
        return new ArrayList<>(allSubscriptions.values());
    }
    
    


    public List<Subscription> getSubscriptionsForType(String eventType) {
        List<Subscription> subs = subscriptions.get(eventType);
        return subs != null ? new ArrayList<>(subs) : new ArrayList<>();
    }
    
    


    public List<Event> getDeadLetterQueue() {
        return new ArrayList<>(deadLetterQueue);
    }
    
    


    public void clearDeadLetterQueue() {
        deadLetterQueue.clear();
    }
    
    


    public List<Event> getReplayBuffer() {
        synchronized (replayBuffer) {
            return new ArrayList<>(replayBuffer);
        }
    }
    
    


    public void replayEvents(String subscriptionId) {
        Subscription subscription = allSubscriptions.get(subscriptionId);
        if (subscription == null) {
            return;
        }
        
        synchronized (replayBuffer) {
            for (Event event : replayBuffer) {
                if (subscription.eventType.equals(event.eventType)) {
                    try {
                        subscription.handler.handleEvent(event);
                        subscription.incrementEventCount();
                    } catch (Exception e) {
                        subscription.handler.onError(e, event);
                    }
                }
            }
        }
    }
    
    


    public EventBusStats getStats() {
        return stats;
    }
    
    


    public EventBusConfig getConfig() {
        return config;
    }
    
    


    public int getQueueSize() {
        return eventQueue.size();
    }
    
    


    public void clearQueue() {
        eventQueue.clear();
    }
    
    


    public void shutdown() {
        shutdown = true;
        
        
        for (Subscription subscription : allSubscriptions.values()) {
            subscription.active = false;
        }
        
        workerExecutor.shutdown();
        try {
            workerExecutor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        clearQueue();
        clearDeadLetterQueue();
        
        synchronized (replayBuffer) {
            replayBuffer.clear();
        }
        
        subscriptions.clear();
        allSubscriptions.clear();
    }
    
    


    public static EventFilter createFilter(Map<String, Object> criteria) {
        return event -> {
            for (Map.Entry<String, Object> entry : criteria.entrySet()) {
                Object value = event.payload.get(entry.getKey());
                if (!Objects.equals(value, entry.getValue())) {
                    return false;
                }
            }
            return true;
        };
    }
    
    


    public static EventFilter createFilter(Predicate<Event> predicate) {
        return predicate::test;
    }
}
