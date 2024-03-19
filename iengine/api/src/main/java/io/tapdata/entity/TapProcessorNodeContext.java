package io.tapdata.entity;

import com.hazelcast.jet.core.Processor;
import com.tapdata.tm.commons.externalStorage.ExternalStorageDto;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.flow.engine.V2.monitor.Monitor;
import io.tapdata.loglistener.TapLogger;
import lombok.Builder;

import java.util.concurrent.atomic.AtomicBoolean;

@Builder
public class TapProcessorNodeContext {
    private ExternalStorageDto externalStorageDto;
    private AtomicBoolean running;
    private TapLogger obsLogger;
    private TapCodecsFilterManager codecsFilterManager;
    private Processor.Context jetContext;
    private Monitor jetJobStatusMonitor;


    public TapCodecsFilterManager getCodecsFilterManager() {
        return codecsFilterManager;
    }

    public void setCodecsFilterManager(TapCodecsFilterManager codecsFilterManager) {
        this.codecsFilterManager = codecsFilterManager;
    }

    public TapLogger getObsLogger() {
        return obsLogger;
    }

    public Monitor getJetJobStatusMonitor() {
        return jetJobStatusMonitor;
    }

    public void setJetJobStatusMonitor(Monitor jetJobStatusMonitor) {
        this.jetJobStatusMonitor = jetJobStatusMonitor;
    }

    public void setObsLogger(TapLogger obsLogger) {
        this.obsLogger = obsLogger;
    }

    public Processor.Context getJetContext() {
        return jetContext;
    }

    public void setJetContext(Processor.Context jetContext) {
        this.jetContext = jetContext;
    }

    public ExternalStorageDto getExternalStorageDto() {
        return externalStorageDto;
    }

    public void setExternalStorageDto(ExternalStorageDto externalStorageDto) {
        this.externalStorageDto = externalStorageDto;
    }

    public AtomicBoolean getRunning() {
        return running;
    }

    public void setRunning(AtomicBoolean running) {
        this.running = running;
    }
}
