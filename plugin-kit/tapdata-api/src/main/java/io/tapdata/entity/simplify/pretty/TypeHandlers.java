package io.tapdata.entity.simplify.pretty;

import io.tapdata.entity.logger.TapLogger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import static io.tapdata.entity.simplify.TapSimplify.list;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/7/29 14:50 Create
 */
public class TypeHandlers<T, R> {
    private static final String TAG = TypeHandlers.class.getSimpleName();
    /**
     * cache all handler of instance class
     */
    private final Map<Class<? extends T>, List<Function<? extends T, R>>> handlers = new ConcurrentHashMap<>();

    /**
     * return all register types
     *
     * @return type list
     */
    public List<Class<? extends T>> keyList() {
        return new ArrayList<>(handlers.keySet());
    }

    /**
     * register handler of type class
     *
     * @param clz handle class type
     * @param fn  handle callback
     */
    public <C extends T> TypeHandlers<T, R> register(Class<C> clz, Function<C, R> fn) {
        handlers.computeIfAbsent(clz, key -> list()).add(fn);
        return this;
    }

    /**
     * handle all function and return first not null value
     *
     * @param ins type instance
     * @return first not null value
     */
    @SuppressWarnings("unchecked")
    public R handle(T ins) {
        if (null != ins) {
            List<Function<? extends T, R>> handlers = this.handlers.get(ins.getClass());
            if (null == handlers) {
                TapLogger.warn(TAG, "Class {} not found corresponding handler, maybe forget to register one? ", ins.getClass());
            } else {
                R result;
                Function<T, R> fn;
                for (Function<? extends T, R> handler : handlers) {
                    fn = (Function<T, R>) handler;
                    if (null != (result = fn.apply(ins))) {
                        return result;
                    }
                }
            }
        }
        return null;
    }

    public static <T, R> TypeHandlers<T, R> create() {
        return new TypeHandlers<>();
    }

}
