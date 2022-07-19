package io.tapdata.common.executor;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by lick on 2019/9/29.
 * Description：
 */
public class ThreadFactory implements java.util.concurrent.ThreadFactory {
    private static final AtomicInteger poolNum = new AtomicInteger(1);
    private final AtomicInteger threadNum = new AtomicInteger(1);
    //set name
    private final String prefix;
    private final boolean daemoThread;
    private final ThreadGroup threadGroup;
    public ThreadFactory(){
        this(null);
    }
    public ThreadFactory(String prefix){
        this(prefix, false);
    }
    public ThreadFactory(String prefix, boolean daemo){
        this.prefix = (prefix != null ? prefix : "StarFish_Thread") + "-" + poolNum.incrementAndGet() + "-thread-";
        daemoThread = daemo;
        SecurityManager s = System.getSecurityManager();
        threadGroup = (s == null) ? Thread.currentThread().getThreadGroup() : s.getThreadGroup();
    }
    @Override
    public Thread newThread(Runnable runnable) {
        String name = prefix + threadNum.getAndIncrement();
        Thread thread = new Thread(threadGroup, runnable, name, 0);
        thread.setDaemon(daemoThread);
        return thread;
    }
}
