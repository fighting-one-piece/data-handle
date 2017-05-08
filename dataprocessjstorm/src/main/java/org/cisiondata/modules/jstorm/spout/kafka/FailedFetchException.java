package org.cisiondata.modules.jstorm.spout.kafka;

@SuppressWarnings("serial")
public class FailedFetchException extends RuntimeException {

    public FailedFetchException(String message) {
        super(message);
    }

    public FailedFetchException(Exception e) {
        super(e);
    }
}
