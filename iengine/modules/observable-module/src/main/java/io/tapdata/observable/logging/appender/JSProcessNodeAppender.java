package io.tapdata.observable.logging.appender;

import com.tapdata.tm.commons.schema.MonitoringLogsDto;

import java.util.Collection;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author GavinXiao
 * @description JSProcessNodeAppender create by Gavin
 * @create 2023/5/11 19:00
 **/
public class JSProcessNodeAppender extends BaseTaskAppender<MonitoringLogsDto> {
    public static final String MAX_LOG_LENGTH_KEY = "maxLogCount_";
    public static final String LOG_LIST_KEY = "logCollector_";

    //日志输出默认100条
    public int defaultLogLength = 100;

    public FixedSizeArrayList<MonitoringLogsDto> logList = new FixedSizeArrayList<>(defaultLogLength);//new FixSizeLinkedList<>(defaultLogLength);
    AtomicReference<Object> logCollector;
    //日志输出上限500条
    public static final int LOG_UPPER_LIMIT = 500;

    private JSProcessNodeAppender(String taskId, AtomicReference<Object> logCollector) {
        super(taskId);
        logCollector.set(logList.elementObj());
        this.logCollector = logCollector;
    }

    public static JSProcessNodeAppender create(String taskId, AtomicReference<Object> logCollector) {
        return new JSProcessNodeAppender(taskId, logCollector);
    }

    public JSProcessNodeAppender maxLogCount(int maxLogCount) {
        if (maxLogCount <= 0) return this;
        defaultLogLength = Math.min(maxLogCount, LOG_UPPER_LIMIT);
        FixedSizeArrayList<MonitoringLogsDto> linkedList = new FixedSizeArrayList<>(defaultLogLength);//new FixSizeLinkedList<>(defaultLogLength);
        if (!logList.isEmpty()) {
            linkedList.addAll(logList);
        }
        logList = linkedList;
        logCollector.set(logList.elementObj());
        return this;
    }

    @Override
    public void append(MonitoringLogsDto log) {
        if (null != log && null != logList) {
            logList.add(log);
        }
    }

    @Override
    public void append(List<MonitoringLogsDto> logs) {
        if (null != logs && null != logList) {
            logList.addAll(logs);
        }
    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }

    public static void main(String[] args) {
        FixedSizeArrayList<Object> list = new FixedSizeArrayList<>(5);
        for (int i = 0; i < 20; i++) {
            final int aaa = i;
            new Thread(() -> {
                list.add(aaa);
                System.out.println( Thread.currentThread().getName() + ":" + list.toString());
            }, "TH-" + i).start();
        }
    }
}
//class FixSizeLinkedList<T> extends CopyOnWriteArrayList<T> {
//    private static final long serialVersionUID = 3292612616231532364L;
//    final transient ReentrantLock lock = new ReentrantLock();;
//
//    // 定义缓存的容量
//    private final int capacity;
//
//    public FixSizeLinkedList(int capacity) {
//        super();
//        this.capacity = capacity;
//    }
//
//    @Override
//    public boolean add(T e) {
//        final ReentrantLock lock = this.lock;
//        lock.lock();
//        try {
//            if (size() + 1 > capacity) {
//                super.remove(0);
//            }
//            return super.add(e);
//        }finally {
//            lock.unlock();
//        }
//    }
//    @Override
//    public boolean addAll (Collection<? extends T> c){
//        final ReentrantLock lock = this.lock;
//        lock.lock();
//        try {
//            final int afterSize = size() + c.size();
//            if (afterSize > capacity) {
//                final int removeSize = afterSize - capacity;
//                subList(0, removeSize).clear();
//            }
//            return super.addAll(c);
//        }finally {
//            lock.unlock();
//        }
//    }
//}

class FixedSizeArrayList<T> {
    private final BlockingDeque<T> deque;

    public FixedSizeArrayList(int size) {
        deque = new LinkedBlockingDeque<>(size);
    }

    public void add(T item) {
        boolean added = deque.offerLast(item);
        if (!added) {
            deque.pollFirst(); // remove oldest element
            deque.offerLast(item); // add newest element
        }
    }

    public T get(int index) {
        return (index >= 0 && index < deque.size()) ? deque.toArray((T[])new Object[0])[index] : null;
    }

    public int size() {
        return deque.size();
    }

    public boolean isEmpty(){
        return deque.isEmpty();
    }

    public boolean addAll(Collection<T> collection){
        collection.forEach(deque::add);
        return true;
    }

    public boolean addAll(FixedSizeArrayList<T> collection){
        collection.deque.forEach(deque::add);
        return true;
    }

    @Override
    public String toString() {
        return deque.toString();
    }

    public Queue<T> elementObj(){
        return deque;
    }
}