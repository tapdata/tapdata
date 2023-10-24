package io.tapdata.entity.utils;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

public class TypeHolder<T> {
    protected final Type type;

    /**
     * Constructs a new type literal. Derives represented class from type
     * parameter.
     *
     * <p>Clients create an empty anonymous subclass. Doing so embeds the type
     * parameter in the anonymous class's type hierarchy so we can reconstitute it
     * at runtime despite erasure.
     */
    protected TypeHolder(){
        Type superClass = getClass().getGenericSuperclass();

        this.type = ((ParameterizedType) superClass).getActualTypeArguments()[0];
    }

    /**
     * Gets underlying {@code Type} instance.
     */
    public Type getType() {
        return type;
    }
}
