package io.tapdata.service.skeleton;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import io.tapdata.entity.annotations.Bean;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.reflection.ClassAnnotationHandler;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.ReflectionUtil;
import io.tapdata.modules.api.net.error.NetErrors;
import io.tapdata.service.skeleton.annotation.RemoteService;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Bean
public class ServiceSkeletonAnnotationHandler extends ClassAnnotationHandler {
    private static final String TAG = ServiceSkeletonAnnotationHandler.class.getSimpleName();
    private ConcurrentHashMap<Long, SkeletonMethodMapping> methodMap = new ConcurrentHashMap<>();

    private Integer serviceVersion;
    private String service;
//    private List<Class<? extends Annotation>> extraAnnotations;
//    private List<ServiceAnnotation> annotationList = new ArrayList<>();

//    @Override
//    public void handlerShutdown() {
//        methodMap.clear();
//    }

    public ServiceSkeletonAnnotationHandler() {
//        extraAnnotations = new ArrayList<>();
    }

//    public void addExtraAnnotation(Class<? extends Annotation> annotationClass) {
//        if (!extraAnnotations.contains(annotationClass)) {
//            extraAnnotations.add(annotationClass);
//        }
//    }

    @Override
    public Class<? extends Annotation> watchAnnotation() {
        return RemoteService.class;
    }

    @Override
    public void handle(Set<Class<?>> classes) throws CoreException {
        if (classes != null && !classes.isEmpty()) {
            StringBuilder uriLogs = new StringBuilder(
                    "\r\n---------------------------------------\r\n");

            ConcurrentHashMap<Long, SkeletonMethodMapping> newMethodMap = new ConcurrentHashMap<>();
            for (Class<?> groovyClass : classes) {
                if (!groovyClass.isInterface() && !Modifier.isAbstract(groovyClass.getModifiers())) {
                    RemoteService requestIntercepting = groovyClass.getAnnotation(RemoteService.class);
                    if (requestIntercepting != null) {
//                        GroovyObjectEx<RemoteService> serverAdapter = getGroovyRuntime()
//                                .create(groovyClass);
                        Object serviceBean = InstanceFactory.bean(groovyClass, true);
                        if (serviceBean != null) {
                            scanClass(groovyClass, serviceBean, newMethodMap);
                        }
                    }
                }
            }
            this.methodMap = newMethodMap;
            uriLogs.append("---------------------------------------");
            TapLogger.debug(TAG, uriLogs.toString());
        }
    }

    public String getService() {
        return service;
    }

    public void setService(String service) {
        this.service = service;
    }

    public Integer getServiceVersion() {
        return serviceVersion;
    }

    public void setServiceVersion(Integer serviceVersion) {
        this.serviceVersion = serviceVersion;
    }

    public class SkeletonMethodMapping extends MethodMapping {
        private Object remoteService;
//        private List<RpcServerInterceptor> rpcServerInterceptors;

        public SkeletonMethodMapping(Method method) {
            super(method);
        }

        private Object[] prepareMethodArgs(MethodRequest request) throws CoreException {
            Object[] rawArgs = request.getArgs();
            if (method == null)
                throw new CoreException(NetErrors.ERROR_METHODMAPPING_METHOD_NULL, "Invoke method is null");
            int argLength = rawArgs != null ? rawArgs.length : 0;
            Object[] args = null;
            if (parameterTypes.length == argLength) {
                args = rawArgs;
            } else if (parameterTypes.length < argLength) {
                args = new Object[parameterTypes.length];
                System.arraycopy(rawArgs, 0, args, 0, parameterTypes.length);
            } else {
                args = new Object[parameterTypes.length];
                if (rawArgs != null)
                    System.arraycopy(rawArgs, 0, args, 0, rawArgs.length);
            }
            return args;
        }

        public MethodResponse invoke(MethodRequest request) throws CoreException {
            Object[] args = prepareMethodArgs(request);
            Long crc = request.getCrc();
            Object returnObj = null;
            CoreException exception = null;
//            String parentTrackId = request.getTrackId();
//            String currentTrackId = null;
//            if (parentTrackId != null) {
//                currentTrackId = ObjectId.get().toString();
//                Tracker tracker = new Tracker(currentTrackId, parentTrackId);
//                Tracker.trackerThreadLocal.set(tracker);
//            }
            StringBuilder builder = new StringBuilder();
            boolean error = false;
            long time = System.currentTimeMillis();
            try {
                builder.append("$$methodrequest:: " + method.getDeclaringClass().getSimpleName() + "#" + method.getName() + " $$service:: " + service + " $$serviceversion:: " + serviceVersion/* + " $$parenttrackid:: " + parentTrackId + " $$currenttrackid:: " + currentTrackId + " $$args:: " + request.getArgsTmpStr()*/);
                returnObj = method.invoke(remoteService, args);
//                returnObj = remoteService.invokeRootMethod(method.getName(), args);
            } catch (Throwable t) {
                error = true;
                builder.append(" $$error" +
                        ":: " + t.getClass() + " $$errormsg:: " + ExceptionUtils.getStackTrace(t));
//                if (t instanceof InvokerInvocationException) {
//                    Throwable theT = ((InvokerInvocationException) t).getCause();
//                    if (theT != null) {
//                        t = theT;
//                    }
//                }
                if(t instanceof InvocationTargetException && t.getCause() != null) {
                    t = t.getCause();
                }
                if (t instanceof CoreException) {
                    exception = (CoreException) t;
                } else {
                    exception = new CoreException(NetErrors.ERROR_METHOD_MAPPING_INVOKE_UNKNOWN_ERROR, t.getMessage());
                }
                TapLogger.error(TAG, "invoke MethodRequest " + request.toString() + " error, " + ExceptionUtils.getStackTrace(t));
            }/* finally {
                String ip = OnlineServer.getInstance().getIp();
                Tracker.trackerThreadLocal.remove();
                long invokeTokes = System.currentTimeMillis() - time;
                builder.append(" $$takes:: " + invokeTokes);
                builder.append(" $$sdockerip:: " + ip);
            }*/
            MethodResponse response = new MethodResponse(returnObj, exception);
            response.setRequest(request);
            response.setVersion(request.getVersion());
            response.setCrc(crc);
//            if (returnObj != null)
//                response.setReturnTmpStr(JSON.toJSONString(returnObj, SerializerFeature.DisableCircularReferenceDetect));
//            builder.append(" $$returnobj:: " + response.getReturnTmpStr());
//            if (error)
//                AnalyticsLogger.error(TAG, builder.toString());
//            else
//                AnalyticsLogger.info(TAG, builder.toString());
            return response;
        }

        public Object getRemoteService() {
            return remoteService;
        }

        public void setRemoteService(Object remoteService) {
            this.remoteService = remoteService;
        }
    }

    public SkeletonMethodMapping getMethodMapping(Long crc) {
        return methodMap.get(crc);
    }

    public void scanClass(Class<?> clazz, Object serverAdapter, ConcurrentHashMap<Long, SkeletonMethodMapping> methodMap) {
        if (clazz == null)
            return;
        RemoteService remoteService = clazz.getAnnotation(RemoteService.class);
        int concurrentLimit = remoteService.concurrentLimit();
        int queueSize = remoteService.waitingSize();
        Method[] methods = ReflectionUtil.getMethods(clazz);
        for (Method method : methods) {
            if (method.isSynthetic() || method.getModifiers() == Modifier.PRIVATE)
                continue;
//                if(method.getDeclaringClass().isAssignableFrom(GroovyObject.class)) {
//                    continue;
//                }
            SkeletonMethodMapping mm = new SkeletonMethodMapping(method);
            mm.setRemoteService(serverAdapter);
            long value = ReflectionUtil.getCrc(method, service);
            if(value == -1) {
                TapLogger.warn(TAG, "Method {} generate crc failed, will be ignored", method.getName());
                continue;
            }
            if (methodMap.containsKey(value)) {
                TapLogger.warn(TAG, "Don't support override methods, please rename your method " + method + " for crc " + value + " and existing method " + methodMap.get(value).getMethod());
                continue;
            }
            Class<?>[] parameterTypes = method.getParameterTypes();
            Type[] genericParamterTypes = method.getGenericParameterTypes();
            if (parameterTypes != null) {
                boolean failed = false;
                for (int i = 0; i < parameterTypes.length; i++) {
                    parameterTypes[i] = ReflectionUtil.getInitiatableClass(parameterTypes[i]);
                    Class<?> parameterType = parameterTypes[i];
                    if (!ReflectionUtil.canBeInitiated(parameterType)) {
                        failed = true;
                        TapLogger.warn(TAG, "Parameter " + parameterType + " in method " + method + " couldn't be initialized. ");
                        break;
                    }
                }
                if (failed)
                    continue;
            }
            mm.setParameterTypes(parameterTypes);
            mm.setGenericParameterTypes(genericParamterTypes);
            Class<?> returnType = method.getReturnType();
            returnType = ReflectionUtil.getInitiatableClass(returnType);
            mm.setReturnClass(returnType);
//                if (method.getGenericReturnType().getTypeName().contains(CompletableFuture.class.getTypeName())) {
//                    mm.setAsync(true);
//                    if (concurrentLimit != -1) {
//                        RpcServerInterceptor concurrentLimitRpcServerInterceptor = new ConcurrentLimitRpcServerInterceptor(concurrentLimit, queueSize, clazz.getName() + "-" + method.getName());
//                        List<RpcServerInterceptor> rpcServerInterceptors = new ArrayList<>();
//                        rpcServerInterceptors.add(concurrentLimitRpcServerInterceptor);
//                        mm.setRpcServerInterceptors(rpcServerInterceptors);
//                    }
//                } else {
                mm.setAsync(false);
//                }
            methodMap.put(value, mm);
            RpcCacheManager.getInstance().putCrcMethodMap(value, service + "_" + clazz.getSimpleName() + "_" + method.getName());

            TapLogger.debug("SCAN", "Mapping crc " + value + " for class " + clazz.getName() + " method " + method.getName() + " for service " + service);
        }
    }


    public ConcurrentHashMap<Long, SkeletonMethodMapping> getMethodMap() {
        return methodMap;
    }

}
