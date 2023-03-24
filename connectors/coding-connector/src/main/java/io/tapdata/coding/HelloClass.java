package io.tapdata.coding;

public interface HelloClass {
    public default void close(){
        System.out.println("close");
    }
}
