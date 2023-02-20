package io.tapdata.coding.utils.http;

public class ErrorHttpException extends RuntimeException{
    public ErrorHttpException(String msg){
        super(msg);
    }
}
