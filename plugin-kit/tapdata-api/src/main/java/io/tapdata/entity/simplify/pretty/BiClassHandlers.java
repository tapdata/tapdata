package io.tapdata.entity.simplify.pretty;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;

public class BiClassHandlers<T, P, R> {
    private static final String TAG = BiClassHandlers.class.getSimpleName();

    private final Map<Class<?>, List<BiFunction<T, P, R>>> classHandlersMap = new ConcurrentHashMap<>();
    public void register(Class<? extends T> tClass, BiFunction<T, P, R> objectHandler) {
        List<BiFunction<T, P, R>> objectHandlers = classHandlersMap.compute(tClass, (aClass, classObjectHandlers) -> new ArrayList<>());
        objectHandlers.add(objectHandler);
    }

    public R handle(T t, P p) {
        if(t != null) {
            List<BiFunction<T, P, R>> objectHandlers = classHandlersMap.get(t.getClass());
            if(objectHandlers != null) {
                for(BiFunction<T, P, R> objectHandler : objectHandlers) {
                    R result = objectHandler.apply(t, p);
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
