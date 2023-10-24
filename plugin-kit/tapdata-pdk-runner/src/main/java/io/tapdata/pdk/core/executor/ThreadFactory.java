package io.tapdata.pdk.core.executor;

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
    private final boolean daemonThread;
    private final ThreadGroup threadGroup;
    public ThreadFactory(){
        this(null);
    }
    public ThreadFactory(String prefix){
        this(prefix, false, null);
    }
    public ThreadFactory(String prefix, ThreadGroup threadGroup){
        this(prefix, false, threadGroup);
    }
    public ThreadFactory(String prefix, boolean daemon, ThreadGroup threadGroup){
        this.prefix = (prefix != null ? prefix : "Thread") + "-" + poolNum.incrementAndGet() + "-thread-";
        daemonThread = daemon;
        this.threadGroup = (threadGroup == null) ? Thread.currentThread().getThreadGroup() : threadGroup;
    }
    @Override
    public Thread newThread(Runnable runnable) {
        String name = prefix + threadNum.getAndIncrement();
        Thread thread = new Thread(threadGroup, runnable, name, 0);
        thread.setDaemon(daemonThread);
        return thread;
    }

    public ThreadGroup getThreadGroup() {
        return threadGroup;
    }
}
