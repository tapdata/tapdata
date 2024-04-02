package io.tapdata.task.skipError;

import org.apache.commons.lang3.StringUtils;

public enum SkipErrorStrategy {
    ERROR_MESSAGE("errorMessage",new ErrorMessageSkip());
    SkipErrorStrategy(String type, SkipError skipError) {
        this.type = type;
        this.skipError = skipError;
    }
    private final String type;
    private final SkipError skipError;

    public String getType() {
        return type;
    }
    public SkipError getSkipError() {
        return skipError;
    }

    public static SkipErrorStrategy getDefaultSkipErrorStrategy(){
        return getSkipErrorStrategy(null);
    }

    public static SkipErrorStrategy getSkipErrorStrategy(String type){
        if(StringUtils.isEmpty(type))return ERROR_MESSAGE;
        for(SkipErrorStrategy skipErrorStrategy: SkipErrorStrategy.values()){
            if(skipErrorStrategy.getType().equals(type)){
                return skipErrorStrategy;
            }
        }
        return ERROR_MESSAGE;
    }
}
