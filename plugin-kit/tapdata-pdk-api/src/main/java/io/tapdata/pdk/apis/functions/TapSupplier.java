package io.tapdata.pdk.apis.functions;
@FunctionalInterface
public interface TapSupplier<T> {

  T get() throws Throwable;
}
