package io.tapdata.bigquery.service.stream.v2;

public class WriteBigQueryException extends RuntimeException{
    public WriteBigQueryException(String msg){
        super(msg);
    }
}
