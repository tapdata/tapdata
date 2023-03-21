package io.tapdata.pdk.tdd.core.base;

public class StreamStopException extends RuntimeException{
    public StreamStopException(String mas){
        super(mas);
    }
}