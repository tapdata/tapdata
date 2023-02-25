package io.tapdata.entity.utils.cache;

public interface Iterator<E> {
    boolean hasNext();

    E next();
}
