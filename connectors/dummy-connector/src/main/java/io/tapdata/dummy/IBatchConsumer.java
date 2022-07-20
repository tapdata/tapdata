package io.tapdata.dummy;

import io.tapdata.dummy.utils.BatchConsumer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/7/10 15:33 Create
 */
public interface IBatchConsumer<T> extends Consumer<T> {

    /**
     * auto batch and push to consumer
     *
     * @param t the input argument
     */
    @Override
    void accept(T t);

    default void flush() {
    }

    /**
     * Clear cache before ending
     */
    static <T> IBatchConsumer<T> getInstance(int batchSize, Consumer<List<T>> consumer) {
        if (batchSize > 1) {
            return new BatchConsumer<>(batchSize, consumer);
        }
        return (t) -> consumer.accept(new ArrayList<>(Collections.singleton(t)));
    }
}
