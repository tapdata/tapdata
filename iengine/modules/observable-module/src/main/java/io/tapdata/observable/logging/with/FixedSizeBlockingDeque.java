package io.tapdata.observable.logging.with;

import java.util.Collection;
import java.util.concurrent.LinkedBlockingDeque;

public class FixedSizeBlockingDeque<T> extends LinkedBlockingDeque<T> {

    public FixedSizeBlockingDeque(int size) {
        super(size);
    }

    public boolean add(T item) {
        boolean added = super.offerLast(item);
        if (!added) {
            super.pollFirst(); // remove oldest element
            super.offerLast(item); // add newest element
        }
        return true;
    }

    public T get(int index) {
        return (index >= 0 && index < super.size()) ? super.toArray((T[]) new Object[0])[index] : null;
    }

    public int size() {
        return super.size();
    }

    public boolean isEmpty() {
        return super.isEmpty();
    }

    public boolean addAll(Collection<? extends T> c) {
        c.forEach(super::add);
        return true;
    }

    public boolean addAll(FixedSizeBlockingDeque<T> collection) {
        collection.forEach(super::add);
        return true;
    }
}