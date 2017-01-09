/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.operation.collect;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.twitter.jsr166e.LongAdder;
import io.crate.breaker.*;
import io.crate.metadata.settings.CrateSettings;
import io.crate.operation.reference.sys.job.JobContext;
import io.crate.operation.reference.sys.job.JobContextLog;
import io.crate.operation.reference.sys.operation.OperationContext;
import io.crate.operation.reference.sys.operation.OperationContextLog;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.node.settings.NodeSettingsService;
import org.elasticsearch.threadpool.ThreadPool;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Stats tables that are globally available on each node and contain meta data of the cluster
 * like active jobs
 * <p>
 * injected via guice instead of using static so that if two nodes run
 * in the same jvm the memoryTables aren't shared between the nodes.
 */
@ThreadSafe
@Singleton
public class StatsTables {

    private final static long QUEUE_CLEAN_INTERVAL = 5L;    // seconds
    private static final NoopRamAccountingQueue NOOP_RAM_ACCOUNTING_QUEUE = NoopRamAccountingQueue.instance();

    private final Map<UUID, JobContext> jobsTable = new ConcurrentHashMap<>();
    private final Map<Tuple<Integer, UUID>, OperationContext> operationsTable = new ConcurrentHashMap<>();

    final AtomicReference<RamAccountingQueue<JobContextLog>> jobsLog;
    final AtomicReference<RamAccountingQueue<OperationContextLog>> operationsLog;

    private final JobsLogIterableGetter jobsLogIterableGetter;
    private final JobsIterableGetter jobsIterableGetter;
    private final OperationsIterableGetter operationsIterableGetter;
    private final OperationsLogIterableGetter operationsLogIterableGetter;
    private final LongAdder activeRequests = new LongAdder();

    protected final NodeSettingsService.Listener listener = new NodeSettingListener();
    private final CircuitBreaker circuitBreaker;
    private final RamAccountingContext ramAccountingContext;
    private int initialOperationsLogSize;
    private int initialJobsLogSize;
    private TimeValue initialJobsLogExpiration;
    private boolean initialIsEnabled;
    volatile int lastOperationsLogSize;
    volatile int lastJobsLogSize;
    volatile TimeValue lastJobsLogExpiration;
    private volatile boolean lastIsEnabled;

    private static final ESLogger LOGGER = Loggers.getLogger(StatsTables.class);

    private final ScheduledExecutorService scheduler;
    private ScheduledFuture scheduledFuture;


    @Inject
    public StatsTables(Settings settings, NodeSettingsService nodeSettingsService, ThreadPool threadPool, CrateCircuitBreakerService breakerService) {
        this(settings, nodeSettingsService, threadPool.scheduler(), breakerService);
    }

    @VisibleForTesting
    StatsTables(Settings settings, NodeSettingsService nodeSettingsService, ScheduledExecutorService scheduledExecutorService, CrateCircuitBreakerService breakerService) {
        circuitBreaker = breakerService.getBreaker(CrateCircuitBreakerService.LOGS);
        ramAccountingContext = new RamAccountingContext("jobsLogContext", circuitBreaker);

        jobsLog = new AtomicReference<>(NOOP_RAM_ACCOUNTING_QUEUE);
        operationsLog = new AtomicReference<>(NOOP_RAM_ACCOUNTING_QUEUE);

        int operationsLogSize = CrateSettings.STATS_OPERATIONS_LOG_SIZE.extract(settings);
        int jobsLogSize = CrateSettings.STATS_JOBS_LOG_SIZE.extract(settings);
        TimeValue jobsLogExpiration = CrateSettings.STATS_JOBS_LOG_EXPIRATION.extractTimeValue(settings);
        boolean isEnabled = CrateSettings.STATS_ENABLED.extract(settings);
        scheduler = scheduledExecutorService;

        if (isEnabled) {
            setJobsLog(jobsLogSize, jobsLogExpiration);
            setOperationsLog(operationsLogSize);
        } else {
            setJobsLog(0, TimeValue.timeValueSeconds(0L));
            setOperationsLog(0);
        }

        initialIsEnabled = isEnabled;
        initialJobsLogSize = jobsLogSize;
        initialJobsLogExpiration = jobsLogExpiration;
        initialOperationsLogSize = operationsLogSize;
        lastOperationsLogSize = operationsLogSize;
        lastJobsLogSize = jobsLogSize;
        lastJobsLogExpiration = jobsLogExpiration;
        lastIsEnabled = isEnabled;


        nodeSettingsService.addListener(listener);
        jobsLogIterableGetter = new JobsLogIterableGetter();
        jobsIterableGetter = new JobsIterableGetter();
        operationsIterableGetter = new OperationsIterableGetter();
        operationsLogIterableGetter = new OperationsLogIterableGetter();
    }

    /**
     * Indicates if statistics are gathered.
     * This result will change if the cluster settings is updated.
     */
    public boolean isEnabled() {
        return lastIsEnabled;
    }

    /**
     * Generate a unique ID for an operation based on jobId and operationId.
     */
    private static Tuple<Integer, UUID> uniqueOperationId(int operationId, UUID jobId) {
        return Tuple.tuple(operationId, jobId);
    }

    /**
     * Track a job. If the job has finished {@link #logExecutionEnd(java.util.UUID, String)}
     * must be called.
     * <p>
     * If {@link #isEnabled()} is false this method won't do anything.
     */
    public void logExecutionStart(UUID jobId, String statement) {
        activeRequests.increment();
        if (!isEnabled()) {
            return;
        }
        jobsTable.put(jobId, new JobContext(jobId, statement, System.currentTimeMillis()));
    }

    /**
     * mark a job as finished.
     * <p>
     * If {@link #isEnabled()} is false this method won't do anything.
     */
    public void logExecutionEnd(UUID jobId, @Nullable String errorMessage) {
        activeRequests.decrement();
        JobContext jobContext = jobsTable.remove(jobId);
        if (!isEnabled() || jobContext == null) {
            return;
        }
        RamAccountingQueue<JobContextLog> jobContextLogs = jobsLog.get();
        jobContextLogs.offer(new JobContextLog(jobContext, errorMessage));
    }

    /**
     * Create a entry into `sys.jobs_log`
     * This method can be used instead of {@link #logExecutionEnd(UUID, String)} if there was no {@link #logExecutionStart(UUID, String)}
     * Call because an error happened during parse, analysis or plan.
     * <p>
     * {@link #logExecutionStart(UUID, String)} is only called after a Plan has been created and execution starts.
     */
    public void logPreExecutionFailure(UUID jobId, String stmt, String errorMessage) {
        RamAccountingQueue<JobContextLog> jobContextLogs = jobsLog.get();
        JobContext jobContext = new JobContext(jobId, stmt, System.currentTimeMillis());
        jobContextLogs.offer(new JobContextLog(jobContext, errorMessage));
    }

    public void operationStarted(int operationId, UUID jobId, String name) {
        if (isEnabled()) {
            operationsTable.put(
                uniqueOperationId(operationId, jobId),
                new OperationContext(operationId, jobId, name, System.currentTimeMillis()));
        }
    }

    public void operationFinished(@Nullable Integer operationId, @Nullable UUID jobId, @Nullable String errorMessage, long usedBytes) {
        if (operationId == null || jobId == null || !isEnabled()) {
            return;
        }
        OperationContext operationContext = operationsTable.remove(uniqueOperationId(operationId, jobId));
        if (operationContext == null) {
            // this might be the case if the stats were disabled when the operation started but have
            // been enabled before the finish
            return;
        }
        operationContext.usedBytes = usedBytes;
        RamAccountingQueue<OperationContextLog> operationContextLogs = operationsLog.get();
        operationContextLogs.offer(new OperationContextLog(operationContext, errorMessage));
    }


    public Supplier<Iterable<?>> jobsGetter() {
        return jobsIterableGetter;
    }

    public Supplier<Iterable<?>> jobsLogGetter() {
        return jobsLogIterableGetter;
    }

    public Supplier<Iterable<?>> operationsGetter() {
        return operationsIterableGetter;
    }

    public Supplier<Iterable<?>> operationsLogGetter() {
        return operationsLogIterableGetter;
    }

    public long activeRequests() {
        return activeRequests.longValue();
    }

    private class JobsLogIterableGetter implements Supplier<Iterable<?>> {

        @Override
        public Iterable<?> get() {
            return jobsLog.get();
        }
    }

    private class JobsIterableGetter implements Supplier<Iterable<?>> {

        @Override
        public Iterable<?> get() {
            return jobsTable.values();
        }
    }

    private class OperationsIterableGetter implements Supplier<Iterable<?>> {

        @Override
        public Iterable<?> get() {
            return operationsTable.values();
        }
    }

    private class OperationsLogIterableGetter implements Supplier<Iterable<?>> {

        @Override
        public Iterable<?> get() {
            return operationsLog.get();
        }
    }

    private void setOperationsLog(int size) {
        if (size == 0) {
            operationsLog.set(NOOP_RAM_ACCOUNTING_QUEUE);
        } else {
            RamAccountingQueue<OperationContextLog> oldQ = operationsLog.get();
            FixedSizeRamAccountingQueue<OperationContextLog> newQ = new FixedSizeRamAccountingQueue<>(ramAccountingContext, size);
            newQ.addAll(oldQ.getQueue());
            operationsLog.set(newQ);
        }
    }

    private void setJobsLog(int size, TimeValue expiration) {
        RamAccountingQueue<JobContextLog> newQ = NOOP_RAM_ACCOUNTING_QUEUE;
        boolean isScheduledQueue = false;

        // Cancel the current jobs log queue scheduler if there is one
        if (scheduledFuture != null) {
            scheduledFuture.cancel(true);
            scheduledFuture = null;
        }

        if (size == 0) {
            if (expiration.getMillis() > 0) {
                newQ = new TimeExpiringRamAccountingQueue<>(ramAccountingContext);
                isScheduledQueue = true;
            } else {
                // use noop
            }
        } else {
            if (expiration.getMillis() > 0) {
                LOGGER.info("Both stats.jobs_log_size and stats.jobs_log_expiration settings are set. Using the latter.");
                newQ = new TimeExpiringRamAccountingQueue<>(ramAccountingContext);
                isScheduledQueue = true;
            } else {
                newQ = new FixedSizeRamAccountingQueue<>(ramAccountingContext, size);
            }
        }
        newQ.addAll(jobsLog.get().getQueue());
        jobsLog.set(newQ);

        if (isScheduledQueue) {
            scheduledFuture = scheduler.scheduleWithFixedDelay(new ScheduledQueueCleaner(), 0L, QUEUE_CLEAN_INTERVAL, TimeUnit.SECONDS);
        }
    }

    private class ScheduledQueueCleaner implements Runnable {
        @Override
        public void run() {
            removeExpiredLogs(jobsLog.get(), System.currentTimeMillis(), lastJobsLogExpiration.getMillis());
        }
    }

    private class NodeSettingListener implements NodeSettingsService.Listener {

        @Override
        public void onRefreshSettings(Settings settings) {
            boolean wasEnabled = lastIsEnabled;
            boolean becomesEnabled = extractIsEnabled(settings);

            if (wasEnabled && becomesEnabled) {
                int opSize = extractOperationsLogSize(settings);
                if (opSize != lastOperationsLogSize) {
                    lastOperationsLogSize = opSize;
                    setOperationsLog(opSize);
                }

                int jobSize = extractJobsLogSize(settings);
                TimeValue jobExpiration = extractJobsLogExpiration(settings);
                if (jobSize != lastJobsLogSize || jobExpiration != lastJobsLogExpiration) {
                    lastJobsLogSize = jobSize;
                    lastJobsLogExpiration = jobExpiration;
                    setJobsLog(jobSize, jobExpiration);
                }

            } else if (wasEnabled) { // !becomesEnabled
                setOperationsLog(0);
                setJobsLog(0, TimeValue.timeValueSeconds(0L));
                lastIsEnabled = false;

                lastOperationsLogSize = extractOperationsLogSize(settings);
                lastJobsLogSize = extractJobsLogSize(settings);
            } else if (becomesEnabled) { // !wasEnabled
                lastIsEnabled = true;

                // queue sizes was zero before so we have to change it
                int opSize = extractOperationsLogSize(settings);
                lastOperationsLogSize = opSize;
                setOperationsLog(opSize);

                int jobSize = extractJobsLogSize(settings);
                TimeValue jobExpiration = extractJobsLogExpiration(settings);
                lastJobsLogSize = jobSize;
                lastJobsLogExpiration = jobExpiration;
                setJobsLog(jobSize, jobExpiration);
            }
        }
    }

    private Integer extractJobsLogSize(Settings settings) {
        return CrateSettings.STATS_JOBS_LOG_SIZE.extract(settings, initialJobsLogSize);
    }

    private TimeValue extractJobsLogExpiration(Settings settings) {
        return CrateSettings.STATS_JOBS_LOG_EXPIRATION.extractTimeValue(settings, initialJobsLogExpiration);
    }

    private Boolean extractIsEnabled(Settings settings) {
        return CrateSettings.STATS_ENABLED.extract(settings, initialIsEnabled);
    }

    private Integer extractOperationsLogSize(Settings settings) {
        return CrateSettings.STATS_OPERATIONS_LOG_SIZE.extract(settings, initialOperationsLogSize);
    }

    static void removeExpiredLogs(RamAccountingQueue<JobContextLog> queue, long currentTimeMillis, long expirationTime) {
        long expired = currentTimeMillis - expirationTime;
        for (JobContextLog log : queue) {
            if (log.ended() < expired) {
                queue.remove();
            } else {
                break;
            }
        }
    }
}
