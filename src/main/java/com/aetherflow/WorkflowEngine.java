package com.aetherflow;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;
import java.util.function.*;






public class WorkflowEngine {
    
    
    public enum TaskState {
        PENDING,
        RUNNING,
        COMPLETED,
        FAILED,
        CANCELLED,
        SKIPPED
    }
    
    
    public enum WorkflowState {
        CREATED,
        RUNNING,
        COMPLETED,
        FAILED,
        CANCELLED,
        PAUSED
    }
    
    
    public static class TaskDefinition {
        public final String taskId;
        public final String taskName;
        public final Supplier<TaskResult> taskExecutor;
        public final Set<String> dependencies;
        public final int timeout;
        public final int maxRetries;
        public final Map<String, Object> parameters;
        public volatile TaskState state;
        public volatile TaskResult result;
        public volatile int retryCount;
        public volatile long startTime;
        public volatile long endTime;
        public final ReentrantLock taskLock;
        
        public TaskDefinition(String taskId, String taskName, Supplier<TaskResult> taskExecutor,
                           Set<String> dependencies, int timeout, int maxRetries,
                           Map<String, Object> parameters) {
            this.taskId = taskId;
            this.taskName = taskName;
            this.taskExecutor = taskExecutor;
            this.dependencies = dependencies != null ? new HashSet<>(dependencies) : new HashSet<>();
            this.timeout = timeout;
            this.maxRetries = maxRetries;
            this.parameters = parameters != null ? new HashMap<>(parameters) : new HashMap<>();
            this.state = TaskState.PENDING;
            this.retryCount = 0;
            this.taskLock = new ReentrantLock();
        }
        
        public boolean canExecute(Map<String, TaskDefinition> allTasks) {
            for (String depId : dependencies) {
                TaskDefinition depTask = allTasks.get(depId);
                if (depTask == null || depTask.state != TaskState.COMPLETED) {
                    return false;
                }
            }
            return true;
        }
        
        public long getExecutionTime() {
            if (startTime == 0) return 0;
            long end = endTime != 0 ? endTime : System.currentTimeMillis();
            return end - startTime;
        }
    }
    
    
    public static class TaskResult {
        public final boolean success;
        public final Object result;
        public final String errorMessage;
        public final Throwable error;
        
        public TaskResult(boolean success, Object result, String errorMessage, Throwable error) {
            this.success = success;
            this.result = result;
            this.errorMessage = errorMessage;
            this.error = error;
        }
        
        public static TaskResult success(Object result) {
            return new TaskResult(true, result, null, null);
        }
        
        public static TaskResult failure(String errorMessage, Throwable error) {
            return new TaskResult(false, null, errorMessage, error);
        }
    }
    
    
    public static class WorkflowDefinition {
        public final String workflowId;
        public final String workflowName;
        public final List<TaskDefinition> tasks;
        public final Map<String, Object> workflowParameters;
        public volatile WorkflowState state;
        public volatile long creationTime;
        public volatile long startTime;
        public volatile long endTime;
        public final ReentrantLock workflowLock;
        
        public WorkflowDefinition(String workflowId, String workflowName, 
                                List<TaskDefinition> tasks, Map<String, Object> workflowParameters) {
            this.workflowId = workflowId;
            this.workflowName = workflowName;
            this.tasks = tasks != null ? new ArrayList<>(tasks) : new ArrayList<>();
            this.workflowParameters = workflowParameters != null ? new HashMap<>(workflowParameters) : new HashMap<>();
            this.state = WorkflowState.CREATED;
            this.creationTime = System.currentTimeMillis();
            this.workflowLock = new ReentrantLock();
        }
        
        public Map<String, TaskDefinition> getTaskMap() {
            Map<String, TaskDefinition> taskMap = new HashMap<>();
            for (TaskDefinition task : tasks) {
                taskMap.put(task.taskId, task);
            }
            return taskMap;
        }
        
        public List<TaskDefinition> getExecutableTasks() {
            Map<String, TaskDefinition> taskMap = getTaskMap();
            List<TaskDefinition> executable = new ArrayList<>();
            
            for (TaskDefinition task : tasks) {
                if (task.state == TaskState.PENDING && task.canExecute(taskMap)) {
                    executable.add(task);
                }
            }
            
            return executable;
        }
        
        public boolean isComplete() {
            for (TaskDefinition task : tasks) {
                if (task.state != TaskState.COMPLETED && task.state != TaskState.SKIPPED) {
                    return false;
                }
            }
            return true;
        }
        
        public boolean hasFailed() {
            for (TaskDefinition task : tasks) {
                if (task.state == TaskState.FAILED) {
                    return true;
                }
            }
            return false;
        }
        
        public long getExecutionTime() {
            if (startTime == 0) return 0;
            long end = endTime != 0 ? endTime : System.currentTimeMillis();
            return end - startTime;
        }
    }
    
    
    public static class WorkflowEngineConfig {
        public int maxConcurrentTasks;
        public long taskTimeout;
        public int maxRetries;
        public long retryDelay;
        public boolean enableParallelExecution;
        public boolean enableTaskTimeout;
        public boolean enableWorkflowPersistence;
        public boolean enableMetrics;
        
        public WorkflowEngineConfig() {
            this.maxConcurrentTasks = 10;
            this.taskTimeout = 300000; 
            this.maxRetries = 3;
            this.retryDelay = 1000; 
            this.enableParallelExecution = true;
            this.enableTaskTimeout = true;
            this.enableWorkflowPersistence = false;
            this.enableMetrics = true;
        }
    }
    
    
    public static class WorkflowEngineStats {
        public final AtomicLong totalWorkflows;
        public final AtomicLong completedWorkflows;
        public final AtomicLong failedWorkflows;
        public final AtomicLong totalTasks;
        public final AtomicLong completedTasks;
        public final AtomicLong failedTasks;
        public final AtomicLong totalRetries;
        public final Map<String, AtomicLong> taskTypeCounts;
        public final Map<String, AtomicLong> errorCounts;
        
        public WorkflowEngineStats() {
            this.totalWorkflows = new AtomicLong(0);
            this.completedWorkflows = new AtomicLong(0);
            this.failedWorkflows = new AtomicLong(0);
            this.totalTasks = new AtomicLong(0);
            this.completedTasks = new AtomicLong(0);
            this.failedTasks = new AtomicLong(0);
            this.totalRetries = new AtomicLong(0);
            this.taskTypeCounts = new ConcurrentHashMap<>();
            this.errorCounts = new ConcurrentHashMap<>();
        }
        
        public void recordWorkflowStart() {
            totalWorkflows.incrementAndGet();
        }
        
        public void recordWorkflowComplete(boolean success) {
            if (success) {
                completedWorkflows.incrementAndGet();
            } else {
                failedWorkflows.incrementAndGet();
            }
        }
        
        public void recordTaskStart(String taskType) {
            totalTasks.incrementAndGet();
            taskTypeCounts.computeIfAbsent(taskType, k -> new AtomicLong(0)).incrementAndGet();
        }
        
        public void recordTaskComplete(boolean success) {
            if (success) {
                completedTasks.incrementAndGet();
            } else {
                failedTasks.incrementAndGet();
            }
        }
        
        public void recordRetry() {
            totalRetries.incrementAndGet();
        }
        
        public void recordError(String errorType) {
            errorCounts.computeIfAbsent(errorType, k -> new AtomicLong(0)).incrementAndGet();
        }
    }
    
    
    private final WorkflowEngineConfig config;
    
    
    private final Map<String, WorkflowDefinition> activeWorkflows;
    
    
    private final Map<String, WorkflowDefinition> completedWorkflows;
    
    
    private final Semaphore taskSemaphore;
    
    
    private final WorkflowEngineStats stats;
    
    
    private final ReentrantLock workflowLock;
    
    
    private final ExecutorService taskExecutor;
    
    
    private volatile boolean shutdown;
    
    
    private volatile Consumer<WorkflowDefinition> workflowCompletionCallback;
    
    


    public WorkflowEngine(WorkflowEngineConfig config) {
        this.config = config;
        this.activeWorkflows = new ConcurrentHashMap<>();
        this.completedWorkflows = new ConcurrentHashMap<>();
        this.taskSemaphore = new Semaphore(config.maxConcurrentTasks);
        this.stats = new WorkflowEngineStats();
        this.workflowLock = new ReentrantLock();
        this.taskExecutor = Executors.newFixedThreadPool(config.maxConcurrentTasks);
        this.shutdown = false;
    }
    
    


    public WorkflowEngine() {
        this(new WorkflowEngineConfig());
    }
    
    


    public void submitWorkflow(WorkflowDefinition workflow) {
        workflowLock.lock();
        try {
            workflow.state = WorkflowState.RUNNING;
            workflow.startTime = System.currentTimeMillis();
            activeWorkflows.put(workflow.workflowId, workflow);
            stats.recordWorkflowStart();
            
            
            executeWorkflow(workflow);
            
        } finally {
            workflowLock.unlock();
        }
    }
    
    


    private void executeWorkflow(WorkflowDefinition workflow) {
        taskExecutor.submit(() -> {
            while (!shutdown && workflow.state == WorkflowState.RUNNING) {
                workflow.workflowLock.lock();
                try {
                    
                    if (workflow.isComplete()) {
                        workflow.state = WorkflowState.COMPLETED;
                        workflow.endTime = System.currentTimeMillis();
                        activeWorkflows.remove(workflow.workflowId);
                        completedWorkflows.put(workflow.workflowId, workflow);
                        stats.recordWorkflowComplete(!workflow.hasFailed());
                        
                        if (workflowCompletionCallback != null) {
                            workflowCompletionCallback.accept(workflow);
                        }
                        break;
                    }
                    
                    
                    if (workflow.hasFailed()) {
                        workflow.state = WorkflowState.FAILED;
                        workflow.endTime = System.currentTimeMillis();
                        activeWorkflows.remove(workflow.workflowId);
                        completedWorkflows.put(workflow.workflowId, workflow);
                        stats.recordWorkflowComplete(false);
                        
                        if (workflowCompletionCallback != null) {
                            workflowCompletionCallback.accept(workflow);
                        }
                        break;
                    }
                    
                    
                    List<TaskDefinition> executableTasks = workflow.getExecutableTasks();
                    
                    if (executableTasks.isEmpty()) {
                        
                        Thread.sleep(100);
                        continue;
                    }
                    
                    
                    if (config.enableParallelExecution) {
                        
                        List<Future<?>> futures = new ArrayList<>();
                        for (TaskDefinition task : executableTasks) {
                            futures.add(taskExecutor.submit(() -> executeTask(workflow, task)));
                        }
                        
                        
                        for (Future<?> future : futures) {
                            try {
                                future.get();
                            } catch (Exception e) {
                                
                            }
                        }
                    } else {
                        
                        for (TaskDefinition task : executableTasks) {
                            executeTask(workflow, task);
                        }
                    }
                    
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    workflow.state = WorkflowState.CANCELLED;
                    break;
                } finally {
                    workflow.workflowLock.unlock();
                }
            }
        });
    }
    
    


    private void executeTask(WorkflowDefinition workflow, TaskDefinition task) {
        task.taskLock.lock();
        try {
            task.state = TaskState.RUNNING;
            task.startTime = System.currentTimeMillis();
            stats.recordTaskStart(task.taskName);
            
            
            taskSemaphore.acquire();
            
            try {
                
                Future<TaskResult> future = taskExecutor.submit(() -> {
                    return task.taskExecutor.get();
                });
                
                TaskResult result;
                if (config.enableTaskTimeout && task.timeout > 0) {
                    result = future.get(task.timeout, TimeUnit.MILLISECONDS);
                } else {
                    result = future.get();
                }
                
                task.result = result;
                
                if (result.success) {
                    task.state = TaskState.COMPLETED;
                    stats.recordTaskComplete(true);
                } else {
                    
                    if (task.retryCount < task.maxRetries) {
                        task.retryCount++;
                        stats.recordRetry();
                        Thread.sleep(config.retryDelay);
                        task.state = TaskState.PENDING;
                    } else {
                        task.state = TaskState.FAILED;
                        stats.recordTaskComplete(false);
                        stats.recordError(result.error != null ? 
                            result.error.getClass().getSimpleName() : "Unknown");
                    }
                }
                
            } catch (TimeoutException e) {
                task.state = TaskState.FAILED;
                task.result = TaskResult.failure("Task timeout", e);
                stats.recordTaskComplete(false);
                stats.recordError("Timeout");
                
            } catch (Exception e) {
                task.state = TaskState.FAILED;
                task.result = TaskResult.failure(e.getMessage(), e);
                stats.recordTaskComplete(false);
                stats.recordError(e.getClass().getSimpleName());
                
            } finally {
                task.endTime = System.currentTimeMillis();
                taskSemaphore.release();
            }
            
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            task.state = TaskState.CANCELLED;
        } finally {
            task.taskLock.unlock();
        }
    }
    
    


    public void cancelWorkflow(String workflowId) {
        workflowLock.lock();
        try {
            WorkflowDefinition workflow = activeWorkflows.get(workflowId);
            if (workflow != null) {
                workflow.state = WorkflowState.CANCELLED;
                workflow.endTime = System.currentTimeMillis();
                
                
                for (TaskDefinition task : workflow.tasks) {
                    if (task.state == TaskState.RUNNING) {
                        task.state = TaskState.CANCELLED;
                    }
                }
                
                activeWorkflows.remove(workflowId);
                completedWorkflows.put(workflowId, workflow);
            }
        } finally {
            workflowLock.unlock();
        }
    }
    
    


    public void pauseWorkflow(String workflowId) {
        workflowLock.lock();
        try {
            WorkflowDefinition workflow = activeWorkflows.get(workflowId);
            if (workflow != null) {
                workflow.state = WorkflowState.PAUSED;
            }
        } finally {
            workflowLock.unlock();
        }
    }
    
    


    public void resumeWorkflow(String workflowId) {
        workflowLock.lock();
        try {
            WorkflowDefinition workflow = activeWorkflows.get(workflowId);
            if (workflow != null && workflow.state == WorkflowState.PAUSED) {
                workflow.state = WorkflowState.RUNNING;
                executeWorkflow(workflow);
            }
        } finally {
            workflowLock.unlock();
        }
    }
    
    


    public WorkflowDefinition getWorkflow(String workflowId) {
        workflowLock.lock();
        try {
            WorkflowDefinition workflow = activeWorkflows.get(workflowId);
            if (workflow == null) {
                workflow = completedWorkflows.get(workflowId);
            }
            return workflow;
        } finally {
            workflowLock.unlock();
        }
    }
    
    


    public Collection<WorkflowDefinition> getActiveWorkflows() {
        workflowLock.lock();
        try {
            return new ArrayList<>(activeWorkflows.values());
        } finally {
            workflowLock.unlock();
        }
    }
    
    


    public Collection<WorkflowDefinition> getCompletedWorkflows() {
        workflowLock.lock();
        try {
            return new ArrayList<>(completedWorkflows.values());
        } finally {
            workflowLock.unlock();
        }
    }
    
    


    public void setWorkflowCompletionCallback(Consumer<WorkflowDefinition> callback) {
        this.workflowCompletionCallback = callback;
    }
    
    


    public WorkflowEngineStats getStats() {
        return stats;
    }
    
    


    public WorkflowEngineConfig getConfig() {
        return config;
    }
    
    


    public void clearCompletedWorkflows() {
        workflowLock.lock();
        try {
            completedWorkflows.clear();
        } finally {
            workflowLock.unlock();
        }
    }
    
    


    public void shutdown() {
        shutdown = true;
        
        
        workflowLock.lock();
        try {
            for (String workflowId : new ArrayList<>(activeWorkflows.keySet())) {
                cancelWorkflow(workflowId);
            }
        } finally {
            workflowLock.unlock();
        }
        
        
        taskExecutor.shutdown();
        try {
            taskExecutor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        activeWorkflows.clear();
        completedWorkflows.clear();
    }
}
