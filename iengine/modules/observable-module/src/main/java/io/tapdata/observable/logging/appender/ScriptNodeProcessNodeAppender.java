package io.tapdata.observable.logging.appender;

import com.tapdata.tm.commons.schema.MonitoringLogsDto;
import io.tapdata.observable.logging.with.FixedSizeBlockingDeque;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author GavinXiao
 * @description ScriptNodeProcessNodeAppender create by Gavin : handle test run log for JS node or python node
 * @create 2023/5/11 19:00
 **/
public class ScriptNodeProcessNodeAppender extends BaseTaskAppender<MonitoringLogsDto> {
    public static final String MAX_LOG_LENGTH_KEY = "ScriptNodeId_";
    public static final String SCRIPT_NODE_ID_KEY = "maxLogCount_";
    public static final String LOG_LIST_KEY = "logCollector_";
    public static final String LOGGER_NAME_PREFIX = "script-node-test-run-log-";

    //日志输出默认100条
    public int defaultLogLength = 100;
    private final Logger logger;
    private Set<String> nodeIds;

    public FixedSizeBlockingDeque<MonitoringLogsDto> logList ;//= new FixedSizeBlockingDeque<>(defaultLogLength);//new FixSizeLinkedList<>(defaultLogLength);
    AtomicReference<Object> logCollector;
    //日志输出上限500条
    public static final int LOG_UPPER_LIMIT = 500;

    private ScriptNodeProcessNodeAppender(String taskId, AtomicReference<Object> logCollector, Integer maxLogCount) {
        super(taskId);
        this.logger = LogManager.getLogger(LOGGER_NAME_PREFIX + taskId);
        this.logCollector = logCollector;
        if (null != logCollector)
            Optional.ofNullable(logCollector.get()).ifPresent(list -> logList = (FixedSizeBlockingDeque<MonitoringLogsDto>) list);
    }

    public ScriptNodeProcessNodeAppender nodeID(String nodes) {
        if (nodeIds == null) {
            nodeIds = new HashSet<>();
        }
        nodeIds.addAll(Arrays.asList(nodes.split(",")));
        return this;
    }

    public static ScriptNodeProcessNodeAppender create(String taskId, AtomicReference<Object> logCollector, Integer maxLogCount) {
        return new ScriptNodeProcessNodeAppender(taskId, logCollector, maxLogCount);
    }

//    public JSProcessNodeAppender maxLogCount(int maxLogCount) {
//        if (maxLogCount <= 0) return this;
//        defaultLogLength = Math.min(maxLogCount, LOG_UPPER_LIMIT);
//        FixedSizeBlockingDeque<MonitoringLogsDto> linkedList = new FixedSizeBlockingDeque<>(defaultLogLength);//new FixSizeLinkedList<>(defaultLogLength);
//        if (!logList.isEmpty()) {
//            linkedList.addAll(logList);
//        }
//        logList = linkedList;
//        logCollector.set(linkedList);
//        return this;
//    }

    @Override
    public void append(MonitoringLogsDto log) {
        if (null != log && null != logList && null != nodeIds && nodeIds.contains(log.getNodeId())) {
            logList.add(log);
        }
    }

    @Override
    public void append(List<MonitoringLogsDto> logs) {
        logs.stream().filter(log -> null != log && null != logList && null != nodeIds && nodeIds.contains(log.getNodeId())).forEach(log -> logList.add(log));
    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {
    }

    public static void main(String[] args) {
        FixedSizeBlockingDeque<Object> list = new FixedSizeBlockingDeque<>(5);
        for (int i = 0; i < 20; i++) {
            final int aaa = i;
            new Thread(() -> {
                list.add(aaa);
                System.out.println(Thread.currentThread().getName() + ":" + list.toString());
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

