package io.tapdata.dummy.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/7/10 15:33 Create
 */
public class BatchConsumer<T> implements IBatchConsumer<T> {
    private final int batchSize;
    private final Consumer<List<T>> batchConsumer;
    private List<T> dataLists = new ArrayList<>();

    public BatchConsumer(int batchSize, Consumer<List<T>> batchConsumer) {
        this.batchSize = batchSize;
        this.batchConsumer = batchConsumer;
    }

    @Override
    public void accept(T t) {
        if (dataLists.size() < batchSize) {
            dataLists.add(t);
        } else {
            flush();
        }
    }

    public void flush() {
        batchConsumer.accept(dataLists);
        dataLists = new ArrayList<>();
    }
}
