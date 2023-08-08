package io.debezium.connector.mysql.utils;

public class GTIDException extends RuntimeException {

    public GTIDException(String message) {
        super(message);
    }

}
