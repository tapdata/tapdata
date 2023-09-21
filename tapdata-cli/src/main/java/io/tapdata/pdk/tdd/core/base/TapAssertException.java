package io.tapdata.pdk.tdd.core.base;

public class TapAssertException extends RuntimeException{
    public TapAssertException(String msg,Throwable e){
        super(msg,e);
    }
    public TapAssertException(Throwable e){
        super(e);
    }
}
