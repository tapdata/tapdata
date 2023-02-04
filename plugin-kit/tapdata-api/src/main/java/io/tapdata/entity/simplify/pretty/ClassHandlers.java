package io.tapdata.entity.simplify.pretty;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class ClassHandlers {
    private static final String TAG = ClassHandlers.class.getSimpleName();

    private final Map<Class<?>, List<Function<?, ?>>> classHandlersMap = new ConcurrentHashMap<>();

    public List<Class<?>> keyList() {
        return new ArrayList<>(classHandlersMap.keySet());
    }
    public <T, R> void register(Class<T> tClass, Function<T, R> objectHandler) {
        List<Function<?, ?>> objectHandlers = classHandlersMap.compute(tClass, (aClass, classObjectHandlers) -> new ArrayList<>());
        objectHandlers.add(objectHandler);
    }

    @SuppressWarnings("unchecked")
    public Object handle(Object t) {
        if(t != null) {
            List<Function<?, ?>> objectHandlers = classHandlersMap.get(t.getClass());
            if(objectHandlers != null) {
                for(Function objectHandler : objectHandlers) {
                     Object result = objectHandler.apply(t);
                    if(result != null)
                        return result;
                }
            } /*else {
                PDKLogger.error(TAG, "Class {} not found corresponding handler, maybe forget to register one? ", t.getClass());
            }*/
        }
        return null;
    }
}
