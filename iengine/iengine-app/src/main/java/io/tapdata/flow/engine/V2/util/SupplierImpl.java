package io.tapdata.flow.engine.V2.util;

import java.util.function.Supplier;


public class SupplierImpl<T> implements Supplier<T> {

    private Supplier<T> supplier;

    public SupplierImpl(Supplier<T> supplier) {
        this.supplier = supplier;
    }

    @Override
    public T get() {
        return supplier.get();
    }
}
