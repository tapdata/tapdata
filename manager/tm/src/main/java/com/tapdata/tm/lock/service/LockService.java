package com.tapdata.tm.lock.service;

public interface LockService {

    boolean lock(String key, long expiration, long sleepMillis);

    boolean release(String key);

    boolean refresh(String key, long expiration);
}