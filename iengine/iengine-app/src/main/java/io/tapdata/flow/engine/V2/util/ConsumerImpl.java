package io.tapdata.flow.engine.V2.util;

import org.jetbrains.annotations.NotNull;

import java.util.function.Consumer;

public class ConsumerImpl<T> implements Consumer<T> {
    private Consumer<T> consumer;

    public ConsumerImpl(Consumer<T> consumer){
        this.consumer = consumer;
    }
    @Override
    public void accept(T t) {
        consumer.accept(t);
    }

    @NotNull
    @Override
    public Consumer<T> andThen(@NotNull Consumer<? super T> after) {
        return Consumer.super.andThen(after);
    }



}
