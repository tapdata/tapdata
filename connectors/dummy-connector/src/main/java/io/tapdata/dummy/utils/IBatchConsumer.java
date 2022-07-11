package io.tapdata.dummy.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/7/10 15:33 Create
 */
public interface IBatchConsumer<T> extends Consumer<T> {

    default void flush() {
    }

    static <T> IBatchConsumer<T> getInstance(int batchSize, Consumer<List<T>> consumer) {
        if (batchSize > 1) {
            return new BatchConsumer<>(batchSize, consumer);
        }
        return (t) -> consumer.accept(new ArrayList<>(Collections.singleton(t)));
    }
}
