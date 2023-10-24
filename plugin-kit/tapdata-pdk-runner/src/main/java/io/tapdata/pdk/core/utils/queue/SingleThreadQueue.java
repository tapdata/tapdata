package io.tapdata.pdk.core.utils.queue;

import io.tapdata.entity.error.CoreException;
import io.tapdata.pdk.core.error.PDKRunnerErrorCodes;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by lick on 2020/11/18.
 * Description：
 */
public class SingleThreadQueue<T> implements Runnable {
    private final ConcurrentLinkedQueue<T> queue = new ConcurrentLinkedQueue<>();
    private ExecutorService threadPoolExecutor;
    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private final AtomicBoolean isStopping = new AtomicBoolean(false);
    private Handler<T> handler;
    private ErrorHandler<T> errorHandler;
    protected String name;
    public SingleThreadQueue(String name){
        this.name = name;
    }
    public SingleThreadQueue<T> withExecutorService(ExecutorService executorService) {
        this.threadPoolExecutor = executorService;
        return this;
    }

    public SingleThreadQueue<T> withHandler(Handler<T> handler) {
        this.handler = handler;
        return this;
    }
    public SingleThreadQueue<T> withErrorHandler(ErrorHandler<T> errorHandler) {
        this.errorHandler = errorHandler;
        return this;
    }

    public void start(){
        if(isStopping.get())
            throw new CoreException(PDKRunnerErrorCodes.COMMON_SINGLE_THREAD_QUEUE_STOPPED, "SingleThreadQueue is stopped");
        if(isRunning.compareAndSet(false, true)){
            threadPoolExecutor.execute(this);
        }
    }
    @Override
    public void run() {
        boolean end = false;
        while (!end){
            if(queue.isEmpty()) {
                synchronized (this) {
                    if(queue.isEmpty()) {
                        isRunning.compareAndSet(true, false);
                        end = true;
                    }
                }
            } else {
                T t = queue.poll();
                if(t != null && !isStopping.get()) {
                    try {
                        this.handler.execute(t);
                    } catch (Throwable e) {
                        if(errorHandler != null) {
                            try {
                                this.errorHandler.error(t, e);
                            } catch(Throwable throwable) {
                                throwable.printStackTrace();
                            }
                        }
                    } finally {
                        consumed(t);
                    }
                }
            }
        }
    }

    protected void consumed(T t) {
    }

    public void offer(T t){
        if(isStopping.get())
            throw new CoreException(PDKRunnerErrorCodes.COMMON_SINGLE_THREAD_QUEUE_STOPPED, "SingleThreadQueue is stopped");
        if(queue.isEmpty()){
            synchronized (this){
                queue.add(t);
            }
        }else {
            queue.add(t);
        }
    }

    public void offerAndStart(T t){
        offer(t);
        start();
    }

    public void stop() {
        if(isStopping.compareAndSet(false, true)) {
            clear();
        }
    }

    public void clear() {
        queue.clear();
    }

    public Handler<T> getHandler() {
        return handler;
    }

    public Queue<T> getQueue() {
        return queue;
    }

    public String getName() {
        return name;
    }
}
