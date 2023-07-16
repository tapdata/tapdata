package io.tapdata.sybase.cdc.dto.watch;

/**
 * @author GavinXiao
 * @description StopFock create by Gavin
 * @create 2023/7/13 12:23
 **/
public class StopLock {
    private boolean isAlive;

    public StopLock(boolean isAlive) {
        this.isAlive = isAlive;
    }

    public synchronized void stop() {
        this.isAlive = false;
    }

    public synchronized boolean isAlive() {
        return this.isAlive;
    }
}
