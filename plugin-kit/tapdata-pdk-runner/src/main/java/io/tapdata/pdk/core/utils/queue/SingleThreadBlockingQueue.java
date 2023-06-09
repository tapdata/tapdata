package io.tapdata.pdk.core.utils.queue;

import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.core.error.PDKRunnerErrorCodes;
import io.tapdata.pdk.core.executor.ExecutorsManager;
import io.tapdata.entity.memory.MemoryFetcher;
import io.tapdata.pdk.core.utils.CommonUtils;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 *
 * @param <T>
 */
public class SingleThreadBlockingQueue<T> implements Runnable, MemoryFetcher {
    private static final String TAG = SingleThreadBlockingQueue.class.getSimpleName();
    private ExecutorService threadPoolExecutor;
    private int maxSize = 20;
    private int maxWaitMilliSeconds = 100;
    private final Object lock = new Object();
    private volatile AtomicBoolean isFull = new AtomicBoolean(false);
    private LinkedBlockingQueue<T> queue;
    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private final AtomicBoolean isStopping = new AtomicBoolean(false);
    private ListHandler<T> listHandler;
    private ListErrorHandler<T> listErrorHandler;
    private int handleSize = 20;
//    private List<T> handleList = new ArrayList<>();
    protected String name;
    private LongAdder counter = new LongAdder();
    private int notifySize;
    private LongAdder notifyCounter = new LongAdder();


//    private SingleThreadQueue<T> ensureSingleThreadInputQueue;

    public SingleThreadBlockingQueue(String name){
        this.name = name;
    }
    public SingleThreadBlockingQueue<T> withExecutorService(ExecutorService executorService) {
        this.threadPoolExecutor = executorService;
        return this;
    }

    /**
     * The batch size when consume data.
     *
     * @param size
     * @return
     */
    public SingleThreadBlockingQueue<T> withHandleSize(int size) {
        handleSize = size;
        return this;
    }

    /**
     * The batch handler for consuming data.
     *
     * @param listHandler
     * @return
     */
    public SingleThreadBlockingQueue<T> withHandler(ListHandler<T> listHandler) {
        this.listHandler = listHandler;
        return this;
    }

    /**
     * The batch handler when error occurred.
     *
     * @param listErrorHandler
     * @return
     */
    public SingleThreadBlockingQueue<T> withErrorHandler(ListErrorHandler<T> listErrorHandler) {
        this.listErrorHandler = listErrorHandler;
        return this;
    }

    /**
     * Queue max size.
     * When reach the max size, the queue will block enqueue thread.
     *
     * @param maxSize
     * @return
     */
    public SingleThreadBlockingQueue<T> withMaxSize(int maxSize) {
        this.maxSize = maxSize;
        return this;
    }

    public SingleThreadBlockingQueue<T> withMaxWaitMilliSeconds(int maxWaitMilliSeconds) {
        this.maxWaitMilliSeconds = maxWaitMilliSeconds;
        return this;
    }
    private void startPrivate(){
        if(isStopping.get())
            throw new CoreException(PDKRunnerErrorCodes.COMMON_SINGLE_THREAD_QUEUE_STOPPED, "SingleThreadBlockingQueue is stopped");
        if(isRunning.compareAndSet(false, true)){
            threadPoolExecutor.execute(this);
        }
    }
    public SingleThreadBlockingQueue<T> start(){
        if(isStopping.get())
            throw new CoreException(PDKRunnerErrorCodes.COMMON_SINGLE_THREAD_QUEUE_STOPPED, "SingleThreadBlockingQueue is stopped");
        if(threadPoolExecutor == null)
            throw new CoreException(PDKRunnerErrorCodes.COMMON_SINGLE_THREAD_BLOCKING_QUEUE_NO_EXECUTOR, "SingleThreadBlockingQueue " + name + " no threadPoolExecutor");

        if(queue == null) {
            queue = new LinkedBlockingQueue<>(maxSize);
            notifySize = maxSize / 2;
        }
        startPrivate();
        return this;
    }
    @Override
    public void run() {
        while (!isStopping.get()) {
            if(queue.isEmpty()) {
                synchronized (lock) {
                    if(queue.isEmpty()) {
                        isRunning.compareAndSet(true, false);
                        break;
                    }
                }
            } else {
                try {
                    List<T> handleList = new ArrayList<>();
//                    synchronized (this) {
                        T t = null;
                        while((t = queue.poll(maxWaitMilliSeconds, TimeUnit.MILLISECONDS)) != null) {
                            handleList.add(t);
                            consumed(t);
                            if(handleList.size() >= handleSize)
                                break;
                        }
//                    }
                    if(!isStopping.get() && !handleList.isEmpty()) {
                        execute(handleList);
                    }
//                    handleList.clear();
                }  catch(Throwable throwable) {
                    throwable.printStackTrace();
                    TapLogger.error(TAG, "{} occurred unknown error, {}", name, throwable.getMessage());
                }
            }
        }
    }

    private void execute(List<T> t) {
        counter.add(t.size());
        try {
            this.listHandler.execute(t);
        } catch (Throwable e) {
            e.printStackTrace();
            if(listErrorHandler != null) {
                CommonUtils.ignoreAnyError(() -> {
                    this.listErrorHandler.error(t, e);
                }, TAG);
            }
        }
    }


    private synchronized void input(T t) {
        boolean full;
        if(queue.isEmpty()){
            synchronized (lock){
                full = queue.offer(t);
            }
        } else {
            full = queue.offer(t);
        }
        if(!full) {
            isFull.set(true);
//                TapLogger.info(TAG, "{} queue is full, wait polling to add more {}", name, queue.size());
            while(isFull.get()) {
                try {
                    this.wait(120000);
                } catch (InterruptedException interruptedException) {
                    interruptedException.printStackTrace();
                    TapLogger.error(TAG, "{} is interrupted, {}", name, interruptedException.getMessage());
                    Thread.currentThread().interrupt();
                }
            }
//                    TapLogger.info(TAG, "wake up to add {}", t);
            offer(t);
        }

    }

    public void add(T t) {
        offer(t);
    }

    public void offer(T t) {
        if(queue == null)
            throw new CoreException(PDKRunnerErrorCodes.COMMON_ILLEGAL_PARAMETERS, "Queue is not initialized");
        if(isStopping.get())
            throw new CoreException(PDKRunnerErrorCodes.COMMON_SINGLE_THREAD_QUEUE_STOPPED, "SingleThreadQueue is stopped");

//        ensureSingleThreadInputQueue.offerAndStart(t);
        input(t);
        startPrivate();
    }

    public void stop() {
        if(isStopping.compareAndSet(false, true)) {
            clear();
        }
    }

    public void clear() {
        queue.clear();
    }

    public ListHandler<T> getHandler() {
        return listHandler;
    }

    public Queue<T> getQueue() {
        return queue;
    }

    public String getName() {
        return name;
    }

    protected synchronized void consumed(T t) {
//        logger.info("queue size {}", getQueue().size());
        if(isFull.get()) {
            notifyCounter.increment();
            if(notifyCounter.longValue() > notifySize || queue.isEmpty()) {
//                logger.info("123 queue size {} notifyCounter {} notifySize {}", getQueue().size(), notifyCounter.longValue(), notifySize);
                if(isFull.compareAndSet(true, false)) {
//                    synchronized (this) {
                        this.notifyAll();
//                    }
                    notifyCounter.reset();
                }
//                logger.info("notifyAll queue size {} notifyCounter {} notifySize {}", getQueue().size(), notifyCounter.longValue(), notifySize);
//                synchronized (lock) {
//                    if(isFull && (notifyCounter.longValue() > notifySize || queue.isEmpty())) {
//
//                    }
//                }

            }
        }
    }

    public static void main(String... args) {
//        LinkedBlockingQueue<String> queue1 = new LinkedBlockingQueue<>(4);
//        for(int i = 0; i < 10; i++) {
//            queue1.add("aaa " + i);
//            logger.info("aaa " + i);
//        }
//        String v = null;
//        while((v = queue1.poll()) != null) {
//            logger.info(v);
//        }
//        if(true)
//            return;
        ExecutorService executorService = Executors.newFixedThreadPool(1);
//        Logger logger = LoggerFactory.getLogger("aaa");
        AtomicLong now = new AtomicLong(System.currentTimeMillis());
        SingleThreadBlockingQueue<String> queue = new SingleThreadBlockingQueue<String>("aaa")
                .withMaxSize(50)
                .withHandleSize(5)
                .withMaxWaitMilliSeconds(100)
                .withExecutorService(ExecutorsManager.getInstance().getExecutorService())
                .withHandler(o -> {
//                    Thread.sleep(10);
                    TapLogger.info(TAG, "handle " + Arrays.toString(o.toArray()));
                })
                .withErrorHandler((o, throwable) -> {
                    TapLogger.error(TAG, "error handle " + Arrays.toString(o.toArray()));
                }).start();
                long time = System.currentTimeMillis();
        for(int i = 0; i < 1000000; i++) {
            final int value = i;
            queue.offer("hello " + value);
//            executorService.submit(() -> {
//                try {
//                    Thread.sleep(10);
//                } catch (InterruptedException interruptedException) {
//                    interruptedException.printStackTrace();
//                }
//            });
        }
        System.out.println("done " + CommonUtils.dateString(new Date(now.get())));
    }

    public long counter() {
        return counter.longValue();
    }

    @Override
    public DataMap memory(String keyRegex, String memoryLevel) {
        return DataMap.create().keyRegex(keyRegex)/*.prefix(this.getClass().getSimpleName())*/
                .kv("name", name)
                .kv("handleSize", handleSize)
                .kv("maxSize", maxSize)
                .kv("maxWaitMilliSeconds", maxWaitMilliSeconds)
                .kv("isFull", isFull.get())
                .kv("queueSize", queue.size())
                .kv("threadPoolExecutor", threadPoolExecutor.toString())
                .kv("isRunning", isRunning)
                .kv("isStopping",isStopping)
                .kv("counter", counter.longValue())
                .kv("notifyCounter", notifyCounter.longValue())
                .kv("notifySize", notifySize);
    }
}
