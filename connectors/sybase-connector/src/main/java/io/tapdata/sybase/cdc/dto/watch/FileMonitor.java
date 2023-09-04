package io.tapdata.sybase.cdc.dto.watch;

import io.tapdata.entity.logger.Log;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import org.apache.commons.io.monitor.FileAlterationListener;
import org.apache.commons.io.monitor.FileAlterationMonitor;
import org.apache.commons.io.monitor.FileAlterationObserver;

import java.io.File;
import java.io.FileFilter;

public class FileMonitor {

    private final FileAlterationMonitor monitor;
    private final StreamReadConsumer cdcConsumer;
    private final Log log;
    private final String monitorPath;
    private final long interval;

    public long getInterval() {
        return interval;
    }

    public StreamReadConsumer getCdcConsumer() {
        return cdcConsumer;
    }

    public FileMonitor(StreamReadConsumer cdcConsumer, long interval, Log log, String monitorPath) {
        monitor = new FileAlterationMonitor(interval);
        this.cdcConsumer = cdcConsumer;
        this.log = log;
        this.monitorPath = monitorPath;
        this.interval = interval;
    }

    /**
     * 给文件添加监听
     *
     * @param path     文件路径
     * @param listener 文件监听器
     */
    public void monitor(String path, FileAlterationListener listener) {
        FileAlterationObserver observer = new FileAlterationObserver(new File(path), fileName ->
            "object_metadata.yaml".equals(fileName.getName())
        );
        observer.addListener(listener);
        monitor.addObserver(observer);
    }

    public void stop() {
        try {
            monitor.stop();
            if (null != log) {
                log.info("Cdc is end and will not monitor about file: {}", monitorPath);
            }
        } catch (Exception e) {
            //log.info("File monitor can not be stop, msg: {}", e.getMessage());
        } finally {
            if (null != cdcConsumer) {
                cdcConsumer.streamReadEnded();
            }
        }
    }

    public void start() throws Exception {
        if (null != cdcConsumer) {
            cdcConsumer.streamReadStarted();
        }
        monitor.start();
        if (null != log) {
            log.info("Cdc is start and will monitor file: {}", monitorPath);
        }
    }
}
