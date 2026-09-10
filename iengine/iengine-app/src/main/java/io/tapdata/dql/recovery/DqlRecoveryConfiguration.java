package io.tapdata.dql.recovery;

import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.mongo.HttpClientMongoOperator;
import com.tapdata.tm.dql.config.DqlRuntimeConfig;
import io.tapdata.common.SettingService;
import io.tapdata.dql.client.DqlTmClient;
import io.tapdata.dql.reporter.DqlEventReporter;
import io.tapdata.flow.engine.V2.schedule.TapdataTaskScheduler;
import io.tapdata.flow.engine.V2.task.impl.HazelcastTaskService;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/** Production wiring for the Engine side of DQL recovery. */
@Configuration
public class DqlRecoveryConfiguration {

    @Bean
    public DqlRecoveryRuntimeRegistry dqlRecoveryRuntimeRegistry() {
        return DqlRecoveryRuntimeRegistry.global();
    }

    @Bean
    public DqlRuntimeConfig dqlRecoveryRuntimeConfig(
            @Qualifier("settingService") SettingService settingService) {
        return DqlRuntimeConfig.from(key -> settingService.getString(key, null));
    }

    @Bean
    public DqlRecoveryEventSource dqlRecoveryEventSource(
            @Qualifier("clientMongoOperator") ClientMongoOperator clientMongoOperator) {
        return new MongoDqlRecoveryEventSource(clientMongoOperator);
    }

    @Bean
    public DqlRecoveryReportSender dqlRecoveryReportSender(
            @Qualifier("clientMongoOperator") ClientMongoOperator clientMongoOperator) {
        if (!(clientMongoOperator instanceof HttpClientMongoOperator httpClientMongoOperator)) {
            throw new IllegalStateException(
                    "DLQ recovery requires the Engine HTTP Mongo operator to report callbacks to TM");
        }
        DqlEventReporter reporter = new DqlEventReporter(new DqlTmClient(httpClientMongoOperator));
        return (command, report) -> reporter.reportRecovery(command.getTaskId(), report);
    }

    @Bean(name = "dqlRecoveryExecutor", destroyMethod = "shutdown")
    public ExecutorService dqlRecoveryExecutor() {
        AtomicInteger sequence = new AtomicInteger();
        ThreadFactory factory = runnable -> {
            Thread thread = new Thread(runnable,
                    "DLQ-Recovery-Worker-" + sequence.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
        return Executors.newCachedThreadPool(factory);
    }

    @Bean
    public DqlRecoveryCoordinator dqlRecoveryCoordinator(
            DqlRecoveryEventSource eventSource,
            DqlRecoveryReportSender reportSender,
            DqlRuntimeConfig runtimeConfig,
            @Qualifier("dqlRecoveryExecutor") ExecutorService executor,
            TapdataTaskScheduler taskScheduler,
            HazelcastTaskService taskService) {
        DqlRecoveryEventSink unavailableSink = event -> {
            throw new IllegalStateException("DLQ recovery source boundary is unavailable");
        };
        DqlRecoveryBarrier unavailableBarrier = (eventId, timeoutMillis) -> {
            throw new IllegalStateException("DLQ recovery barrier is unavailable");
        };
        if (HazelcastTaskService.getHazelcastInstance() == null) {
            throw new IllegalStateException("DLQ recovery requires an initialized Hazelcast instance");
        }
        DqlRecoveryBatchRuntimeFactory batchRuntimeFactory = new HazelcastDqlRecoveryBatchRuntimeFactory(
                new TapdataDqlRecoveryTaskLifecycle(taskScheduler),
                taskService,
                HazelcastTaskService.getHazelcastInstance());
        return new DqlRecoveryCoordinatorImpl(
                eventSource,
                unavailableSink,
                unavailableBarrier,
                reportSender,
                DqlRecoveryExecutionPolicy.from(runtimeConfig),
                runtimeConfig.getRecoveryEventTimeoutSeconds() * 1_000L,
                executor,
                command -> null,
                command -> null,
                (command, sourceBoundary) -> unavailableBarrier,
                runtimeConfig,
                DqlRecoveryCoordinatorImpl.DEFAULT_HEARTBEAT_EXECUTOR,
                batchRuntimeFactory
        );
    }
}
