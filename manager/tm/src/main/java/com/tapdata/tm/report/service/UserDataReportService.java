package com.tapdata.tm.report.service;

import com.tapdata.tm.report.dto.*;
import com.tapdata.tm.report.service.platform.ReportPlatform;
import com.tapdata.tm.report.util.UserDataReportUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.client.HttpClientBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.concurrent.*;

@Service
@Slf4j
public class UserDataReportService {
    public static final String CONFIGURE_DATASOURCE_EVENT = "configure_datasource_event";
    public static final String RUNS_NUM_EVENT = "runs_num_event";
    public static final String RUN_DAYS_EVENT = "run_days_event";
    public static final String UNIQUE_INSTALL_EVENT = "unique_install_event";
    public static final String TASKS_NUM_EVENT = "tasks_num_event";

    private static String machineId = UserDataReportUtils.generateMachineId();

    @Value("${report.data.oss}")
    private boolean acceptReportData;
    protected static LinkedBlockingQueue reportQueue = new LinkedBlockingQueue(100);
    @Autowired
    private ReportPlatform reportPlatform;

    @PostConstruct
    protected void initReportDataThread(){
        if (!acceptReportData) return;
        ExecutorService executorService = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new SynchronousQueue<>());
        CompletableFuture.runAsync(() -> {
            Thread.currentThread().setName("report-user-data-thread");
            Object reportData;
            while (!Thread.currentThread().isInterrupted()){
                try {
                    reportData = reportQueue.poll(1, TimeUnit.SECONDS);
                    if (null == reportData) continue;
                    consumeData(reportData);
                }catch (InterruptedException e){
                    Thread.currentThread().interrupt();
                } catch (Exception e){
                    log.info("report event poll failed", e);
                }
            }
        }, executorService);
    }
    protected void consumeData(Object reportData) {
        if (reportData instanceof RunsNumBatch){
            runsNum((RunsNumBatch) reportData);
        } else if (reportData instanceof RunDaysBatch){
            runDays((RunDaysBatch) reportData);
        } else if (reportData instanceof UniqueInstallBatch){
            uniqueInstall((UniqueInstallBatch) reportData);
        } else if (reportData instanceof ConfigureSourceBatch){
            configureDatasource((ConfigureSourceBatch) reportData);
        } else if (reportData instanceof TasksNumBatch){
            tasksNum((TasksNumBatch) reportData);
        }
    }

    public void produceData(Object object){
        if (!acceptReportData) return;
        try {
            reportQueue.offer(object, 100, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    protected void runsNum(RunsNumBatch runsNumBatch) {
        if (!acceptReportData) return;
        runsNumBatch.setMachineId(machineId);
        reportPlatform.sendRequest(RUNS_NUM_EVENT, runsNumBatch.toString());
    }
    protected void runDays(RunDaysBatch runDaysBatch) {
        if (!acceptReportData) return;
        runDaysBatch.setMachineId(machineId);
        reportPlatform.sendRequest(RUN_DAYS_EVENT, runDaysBatch.toString());
    }
    protected void uniqueInstall(UniqueInstallBatch uniqueInstallBatch) {
        if (!acceptReportData) return;
        uniqueInstallBatch.setMachineId(machineId);
        reportPlatform.sendRequest(UNIQUE_INSTALL_EVENT, uniqueInstallBatch.toString());
    }
    protected void configureDatasource(ConfigureSourceBatch configureSourceBatch) {
        if (!acceptReportData) return;
        configureSourceBatch.setMachineId(machineId);
        reportPlatform.sendRequest(CONFIGURE_DATASOURCE_EVENT, configureSourceBatch.toString());
    }
    protected void tasksNum(TasksNumBatch tasksNumBatch) {
        if (!acceptReportData) return;
        tasksNumBatch.setMachineId(machineId);
        reportPlatform.sendRequest(TASKS_NUM_EVENT, tasksNumBatch.toString());
    }

}
