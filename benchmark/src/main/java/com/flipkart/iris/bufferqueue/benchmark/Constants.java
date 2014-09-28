package com.flipkart.iris.bufferqueue.benchmark;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

public class Constants {

    public static final String ZERO_LENGTH_MESSAGE_KEY = "ZERO_LENGTH_MESSAGE";
    public static final String VERY_SHORT_MESSAGE_KEY = "VERY_SHORT_MESSAGE";
    public static final String SHORT_MESSAGE_KEY = "SHORT_MESSAGE";
    public static final String MEDIUM_MESSAGE_KEY = "MEDIUM_MESSAGE";
    public static final String LONG_MESSAGE_KEY = "LONG_MESSAGE";
    public static final String VERY_LONG_MESSAGE_KEY = "VERY_LONG_MESSAGE";

    public static final String VERY_SHORT_MESSAGE = "!";
    public static final String SHORT_MESSAGE = "hello world!";
    public static final String MEDIUM_MESSAGE = "this is 1KB str this is 1KB str this is 1KB str this is 1KB str this is 1KB str this is 1KB str this is 1KB str this is 1KB str this is 1KB str this is 1KB str this is 1KB str this is 1KB str this is 1KB str this is 1KB str this is 1KB str this is 1KB str this is 1KB str this is 1KB str this is 1KB str this is 1KB str this is 1KB str this is 1KB str this is 1KB str this is 1KB str this is 1KB str this is 1KB str this is 1KB str this is 1KB str this is 1KB str this is 1KB str this is 1KB str this is 1KB str this is 1KB str this is 1KB str this is 1KB str this is 1KB str this is 1KB str this is 1KB str this is 1KB str this is 1KB str this is 1KB str this is 1KB str this is 1KB str this is 1KB str this is 1KB str this is 1KB str this is 1KB str this is 1KB str this is 1KB str this is 1KB str this is 1KB str this is 1KB str this is 1KB str this is 1KB str this is 1KB str this is 1KB str this is 1KB str this is 1KB str this is 1KB str this is 1KB str this is 1KB str this is 1KB str this is 1KB str this is 1KB str";
    public static final String LONG_MESSAGE = MEDIUM_MESSAGE + MEDIUM_MESSAGE + MEDIUM_MESSAGE + MEDIUM_MESSAGE;
    public static final String VERY_LONG_MESSAGE = LONG_MESSAGE + LONG_MESSAGE + LONG_MESSAGE + LONG_MESSAGE;

    private static final Map<String, String> msgs = new ImmutableMap.Builder<String, String>()
            .put(ZERO_LENGTH_MESSAGE_KEY, "")
            .put(VERY_SHORT_MESSAGE_KEY, VERY_SHORT_MESSAGE)
            .put(SHORT_MESSAGE_KEY, SHORT_MESSAGE)
            .put(MEDIUM_MESSAGE_KEY, MEDIUM_MESSAGE)
            .put(LONG_MESSAGE_KEY, LONG_MESSAGE)
            .put(VERY_LONG_MESSAGE_KEY, VERY_LONG_MESSAGE)
            .build();

    public static byte[] getMessage(String key) {
        return msgs.get(key).getBytes();
    }
}
