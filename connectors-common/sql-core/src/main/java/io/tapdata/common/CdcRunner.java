package io.tapdata.common;

public interface CdcRunner extends Runnable {
    void startCdcRunner() throws Throwable;
    void closeCdcRunner() throws Throwable;
    boolean isRunning() throws Throwable;
}
