package io.tapdata.pdk.core.monitor;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.memory.MemoryFetcher;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.core.api.Node;
import io.tapdata.pdk.core.entity.params.PDKMethodInvoker;
import io.tapdata.pdk.core.error.PDKRunnerErrorCodes;
import io.tapdata.pdk.core.executor.ExecutorsManager;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.pdk.core.utils.RetryUtils;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * TODO start monitor thread for checking slow invocation
 */
public class PDKInvocationMonitor implements MemoryFetcher {
    private static final String TAG = PDKInvocationMonitor.class.getSimpleName();
    private static volatile PDKInvocationMonitor instance = new PDKInvocationMonitor();
    private static final Object lock = new int[0];

    private Map<PDKMethod, InvocationCollector> methodInvocationCollectorMap = new ConcurrentHashMap<>();
    private Consumer<String> errorListener;

    private static Map<Node,List<PDKMethodInvoker>> nodeStopInvokerMap = new ConcurrentHashMap<>();
    public void invokerEnter(Node node,PDKMethodInvoker invoker){
        if (null == node || invoker == null){
            return;
        }
        nodeStopInvokerMap.computeIfAbsent(node, list-> new CopyOnWriteArrayList<>()).add(invoker);
    }

    public static void release(Node releaseNode,PDKMethodInvoker releaseInvoker){
        if (releaseNode != null) {
            List<PDKMethodInvoker> list = nodeStopInvokerMap.get(releaseNode);
            if (null != releaseInvoker && null != list && !list.isEmpty()){
                releaseInvoker.cancelRetry();
                list.remove(releaseInvoker);
            }
        }
    }

    public static void stop(Node closeNode){
        List<PDKMethodInvoker> invokerList = nodeStopInvokerMap.get(closeNode);
        if ( !( null == invokerList || invokerList.isEmpty() ) ) {
            for (PDKMethodInvoker pdkMethodInvoker : invokerList) {
                pdkMethodInvoker.cancelRetry();
            }
        }
        nodeStopInvokerMap.remove(closeNode);
    }

    private PDKInvocationMonitor() {}

    public void setErrorListener(Consumer<String> errorListener) {
        this.errorListener = errorListener;
    }

    public static PDKInvocationMonitor getInstance() {
        return instance;
    }

    public static void invoke(Node node, PDKMethod method, CommonUtils.AnyError r, String logTag, Consumer<CoreException> errorConsumer) {
        instance.invokePDKMethod(node, method, r, null, logTag, errorConsumer, false, 0, 0);
    }
    public static void invoke(Node node, PDKMethod method, CommonUtils.AnyError r, String logTag) {
        instance.invokePDKMethod(node, method, r, null, logTag);
    }
    public static void invoke(Node node, PDKMethod method, CommonUtils.AnyError r, String message, String logTag) {
        instance.invokePDKMethod(node, method, r, message, logTag, null, false, 0, 0);
    }
    public static void invoke(Node node, PDKMethod method, CommonUtils.AnyError r, String message, String logTag, Consumer<CoreException> errorConsumer) {
        instance.invokePDKMethod(node, method, r, message, logTag, errorConsumer, false, 0, 0);
    }
    public static void invoke(Node node, PDKMethod method, CommonUtils.AnyError r, String message, final String logTag, Consumer<CoreException> errorConsumer, boolean async, long retryTimes, long retryPeriodSeconds) {
        instance.invokePDKMethod(node, method, r, message, logTag, errorConsumer, async, null, retryTimes, retryPeriodSeconds);
    }
    public static void invoke(Node node, PDKMethod method, CommonUtils.AnyError r, String message, final String logTag, Consumer<CoreException> errorConsumer, boolean async, ClassLoader contextClassLoader, long retryTimes, long retryPeriodSeconds) {
        instance.invokePDKMethod(node, method, r, message, logTag, errorConsumer, async, contextClassLoader, retryTimes, retryPeriodSeconds);
    }

    public static void invoke(Node node, PDKMethod method, PDKMethodInvoker invoker){
        instance.invokePDKMethod(node, method, invoker);
    }

    public void invokePDKMethod(Node node, PDKMethod method, CommonUtils.AnyError r, String logTag) {
        invokePDKMethod(node, method, r, null, logTag);
    }
    public void invokePDKMethod(Node node, PDKMethod method, CommonUtils.AnyError r, String message, String logTag) {
        invokePDKMethod(node, method, r, message, logTag, null, false, 0, 0);
    }
    public void invokePDKMethod(Node node, PDKMethod method, CommonUtils.AnyError r, String message, String logTag, Consumer<CoreException> errorConsumer) {
        invokePDKMethod(node, method, r, message, logTag, errorConsumer, false, 0, 0);
    }
    public void invokePDKMethod(Node node, PDKMethod method, CommonUtils.AnyError r, String message, final String logTag, Consumer<CoreException> errorConsumer, boolean async, long retryTimes, long retryPeriodSeconds) {
        invokePDKMethod(node, method, r, message, logTag, errorConsumer, async, null, retryTimes, retryPeriodSeconds);
    }
    public void invokePDKMethod(Node node, PDKMethod method, CommonUtils.AnyError r, String message, final String logTag, Consumer<CoreException> errorConsumer, boolean async, ClassLoader contextClassLoader, long retryTimes, long retryPeriodSeconds) {
        if(async) {
            ExecutorsManager.getInstance().getExecutorService().execute(() -> {
                if(contextClassLoader != null)
                    Thread.currentThread().setContextClassLoader(contextClassLoader);
                if(retryTimes > 0) {
                    CommonUtils.autoRetryAsync(() ->
                            node.applyClassLoaderContext(() ->
                                    invokePDKMethodPrivate(method, r, message, logTag, errorConsumer)), logTag, message, retryTimes, retryPeriodSeconds);
                } else {
                    node.applyClassLoaderContext(() -> invokePDKMethodPrivate(method, r, message, logTag, errorConsumer));
                }
            });
        } else {
            node.applyClassLoaderContext(() -> invokePDKMethodPrivate(method, r, message, logTag, errorConsumer));
        }
    }
    public void invokePDKMethod(Node node, PDKMethod method, PDKMethodInvoker invoker) {
        CommonUtils.AnyError r = invoker.getR();
        String message = invoker.getMessage();
        final String logTag = invoker.getLogTag();
        Consumer<CoreException> errorConsumer = invoker.getErrorConsumer();
        boolean async = invoker.isAsync();
        ClassLoader contextClassLoader = invoker.getContextClassLoader();
        if (!this.invokerRetrySetter(invoker)){
            TapLogger.debug(logTag, "Do not retry : maxRetryTimeMinute {}, retryPeriodSeconds {}, retryTimes {}. ",invoker.getMaxRetryTimeMinute(),invoker.getRetryPeriodSeconds(),invoker.getRetryTimes());
            return;
        }
        long retryTimes = invoker.getRetryTimes();
        try {
            this.invokerEnter(node,invoker);
            if (async) {
                ExecutorsManager.getInstance().getExecutorService().execute(() -> {
                    if (contextClassLoader != null) {
                        Thread.currentThread().setContextClassLoader(contextClassLoader);
                    }
                    if (retryTimes > 0) {
                        RetryUtils.autoRetry(node, method, invoker.runnable(() -> node.applyClassLoaderContext(() -> invokePDKMethodPrivate(method, r, message, logTag, errorConsumer))));
                    } else {
                        node.applyClassLoaderContext(() -> invokePDKMethodPrivate(method, r, message, logTag, errorConsumer));
                    }
                });
            } else {
                RetryUtils.autoRetry(node, method, invoker.runnable(() -> node.applyClassLoaderContext(() -> invokePDKMethodPrivate(method, r, message, logTag, errorConsumer))));
            }
        }finally {
            PDKInvocationMonitor.release(node,invoker);
        }
    }
    private void invokePDKMethodPrivate(PDKMethod method, CommonUtils.AnyError r, String message, String logTag, Consumer<CoreException> errorConsumer) {
        String invokeId = methodStart(method, logTag);
        Throwable theError = null;
        try {
            r.run();
        } catch(CoreException coreException) {
            theError = coreException;

            if(errorConsumer != null) {
                errorConsumer.accept(coreException);
            } else {
                if(errorListener != null)
                    errorListener.accept(describeError(method, coreException, message, logTag));
                throw coreException;
            }
        } catch(Throwable throwable) {
            theError = throwable;

            CoreException coreException = new CoreException(PDKRunnerErrorCodes.COMMON_UNKNOWN, throwable, throwable.getMessage());
            if(errorConsumer != null) {
                errorConsumer.accept(coreException);
            } else {
                if(errorListener != null)
                    errorListener.accept(describeError(method, throwable, message, logTag));
                throw coreException;
            }
        } finally {
            methodEnd(method, invokeId, theError, message, logTag);
        }
    }

    private String describeError(PDKMethod method, Throwable throwable, String message, String logTag) {
        return logTag + ": Invoke PDKMethod " + method.name() + " failed, error " + throwable.getMessage() + " context message " + message;
    }

    public String methodStart(PDKMethod method, String logTag) {
        final String invokeId = CommonUtils.processUniqueId();
        InvocationCollector collector = methodInvocationCollectorMap.computeIfAbsent(method, InvocationCollector::new);
        collector.getInvokeIdTimeMap().put(invokeId, System.currentTimeMillis());
//        TapLogger.info(logTag, "methodStart {} invokeId {}", method, invokeId);
        return invokeId;
    }

    public Long methodEnd(PDKMethod method, String invokeId, Throwable error, String message, String logTag) {
        InvocationCollector collector = methodInvocationCollectorMap.get(method);
        if(collector != null) {
            Long time = collector.getInvokeIdTimeMap().remove(invokeId);
            if(time != null) {
                collector.getCounter().increment();
                long takes = System.currentTimeMillis() - time;
                collector.getTotalTakes().add(takes);
                if(error != null && logTag != null) {
                    TapLogger.info(logTag, "methodEnd - {} | message - ({})", method, error.getMessage());//ExceptionUtils.getStackTrace(error)
                    //throw new CoreException(PDKRunnerErrorCodes.COMMON_UNKNOWN, error.getMessage(), error);
                } else {
//                    TapLogger.info(logTag, "methodEnd {} invokeId {} successfully, message {} takes {}", method, invokeId, message, takes);
                }
                return takes;
            }
        }
        return null;
    }

    public static void main(String... args) {
        long time = System.currentTimeMillis();
        for(int i = 0; i < 1000000; i++) {
            UUID.randomUUID().toString();
        }
        System.out.println("takes " + (System.currentTimeMillis() - time));

        AtomicLong counter = new AtomicLong(0);
        time = System.currentTimeMillis();
        String id = null;
        for(int i = 0; i < 1000000; i++) {
            id = Long.toHexString(System.currentTimeMillis()) + Long.toHexString(counter.getAndIncrement());
        }
        System.out.println("takes " + (System.currentTimeMillis() - time) + " id " + id);
    }

    @Override
    public DataMap memory(String keyRegex, String memoryLevel) {
        DataMap dataMap = DataMap.create().keyRegex(keyRegex)/*.prefix(this.getClass().getSimpleName())*/;
        for(Map.Entry<PDKMethod, InvocationCollector> entry : methodInvocationCollectorMap.entrySet()) {
            if(keyRegex != null && !keyRegex.isEmpty() && !keyRegex.contains(entry.getKey().name()))
                continue;
            dataMap.kv(entry.getKey().name(), entry.getValue().memory(keyRegex, memoryLevel));
        }
        return dataMap;
    }

    /**
     最大重试时间----最大重试次数----重试间隔时间
     |-1.最大重试时间小于0:
        |----1.重试时间、重试间隔大于0 ，计算最大重试时间，重试
        |----2.重试时间、重试间隔小于等于0 ，不重试
     |-2.最大重试时间大于0：
        |----1.重试间隔大于0，求重试次数，重试
        |----2.重试间隔小于0,：
            |----1.重试次数大于0，计算重试间隔，重试。
            |----2.重试次数小于0，不重试。
     -----------------------------------------------
     maxRetryTimeMinute - retryPeriodSeconds - retryTimes
     |-1. maxRetryTimeMinute is less than or equal to zero:
         |----1. retryTimes and retryPeriodSeconds is greater than 0. Calculate the maxRetryTimeMinute and retry.
         |----2. retryTimes or retryPeriodSeconds is less than or equal to 0, do not retry.
     |-2. The maxRetryTimeMinute is greater than zero:
        |----1. retryPeriodSeconds is greater than zero. Find retryTimes and try again.
        |----2. retryPeriodSeconds is less than or equal to zero:
            |----1. If retryTimes is greater than zero, calculate the retryPeriodSeconds and retry.
            |----2. If retryTimes is less than or equal to zero, do not retry.
     */
//    private static final long MAX_RETRY_TIMES = 10000L;
//    private static final long MAX_RETRY_PERIOD_SECOND = 100000L;
//    private static final long MAX_MAX_RETRY_TIME_MINUTE = 100000L;
    public static boolean invokerRetrySetter(PDKMethodInvoker invoker){
//        if (invoker.getRetryTimes()>MAX_RETRY_TIMES){
//            invoker.setRetryTimes(MAX_RETRY_TIMES);
//        }
//        if (invoker.getRetryPeriodSeconds()>MAX_RETRY_PERIOD_SECOND){
//            invoker.setRetryPeriodSeconds(MAX_RETRY_PERIOD_SECOND);
//        }
//        if (invoker.getMaxRetryTimeMinute()>MAX_MAX_RETRY_TIME_MINUTE){
//            invoker.setMaxRetryTimeMinute(MAX_MAX_RETRY_TIME_MINUTE);
//        }
//        if(!invoker.isAsync()){
//            return;
//        }
        long maxRetryTimeMinute = invoker.getMaxRetryTimeMinute();
        long retryPeriodSeconds = invoker.getRetryPeriodSeconds();
        long retryTimes = invoker.getRetryTimes();
        if (maxRetryTimeMinute > 0) {//最大重试时间大于0
            if (retryPeriodSeconds>0) {//重试间隔时间大于0
                //计算重试次数，向下取整
                invoker.setRetryTimes(maxRetryTimeMinute*60 / retryPeriodSeconds);
                return Boolean.TRUE;
            }else {
                if (retryTimes>0){
                    invoker.setRetryPeriodSeconds(maxRetryTimeMinute*60 / retryTimes);
                    return Boolean.TRUE;
                }else {
                    throw new IllegalArgumentException("RetryPeriodSeconds can not be zero or less than zero.");
                }
            }
        }else {
            if (retryPeriodSeconds>0&&retryTimes>0){
                invoker.setMaxRetryTimeMinute(retryPeriodSeconds*retryTimes);
                return Boolean.TRUE;
            }
        }
        return Boolean.FALSE;
    }
}
