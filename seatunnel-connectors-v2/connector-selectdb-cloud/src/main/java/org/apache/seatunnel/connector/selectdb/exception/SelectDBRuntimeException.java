package org.apache.seatunnel.connector.selectdb.exception;

public class SelectDBRuntimeException extends RuntimeException{
    public SelectDBRuntimeException() {
    }

    public SelectDBRuntimeException(String message) {
        super(message);
    }

    public SelectDBRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }

    public SelectDBRuntimeException(Throwable cause) {
        super(cause);
    }

    public SelectDBRuntimeException(String message, Throwable cause,
                                    boolean enableSuppression,
                                    boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
