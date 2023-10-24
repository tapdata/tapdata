package io.tapdata.entity.result;

import java.util.ArrayList;
import java.util.List;

public class TapResult<T> {
    public static final int RESULT_SUCCESSFULLY = 1;
    public static final int RESULT_SUCCESSFULLY_WITH_WARN = 2;
    public static final int RESULT_FAILED = 10;

    private int result = RESULT_SUCCESSFULLY;
    public TapResult<T> result(int result) {
        switch (result) {
            case RESULT_FAILED:
            case RESULT_SUCCESSFULLY:
            case RESULT_SUCCESSFULLY_WITH_WARN:
                this.result = result;
                break;
            default:
                throw new IllegalArgumentException("Result is illegal for TapResult, " + result);
        }
        return this;
    }

    private T data;
    public TapResult<T> data(T data) {
        this.data = data;
        return this;
    }

    private List<ResultItem> resultItems;

    public TapResult<T> addItem(ResultItem resultItem) {
        if(resultItems == null) {
            resultItems = new ArrayList<>();
        }
        resultItems.add(resultItem);
        return this;
    }

    public static <T> TapResult<T> successfully() {
        return successfully(null);
    }

    public static <T> TapResult<T> successfully(T data) {
        TapResult<T> result = new TapResult<>();
        return result.data(data).result(RESULT_SUCCESSFULLY);
    }

    public static <T> TapResult<T> successfullyWithWarn() {
        return successfullyWithWarn(null);
    }

    public static <T> TapResult<T> successfullyWithWarn(T data) {
        TapResult<T> result = new TapResult<>();
        return result.data(data).result(RESULT_SUCCESSFULLY_WITH_WARN);
    }

    public static <T> TapResult<T> failed() {
        TapResult<T> result = new TapResult<>();
        return result.result(RESULT_FAILED);
    }

    public boolean isSuccessfully() {
        return result == RESULT_SUCCESSFULLY || result == RESULT_SUCCESSFULLY_WITH_WARN;
    }

    public boolean hasWarning() {
        return result == RESULT_SUCCESSFULLY_WITH_WARN;
    }

    public int getResult() {
        return result;
    }

    public void setResult(int result) {
        this.result = result;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }

    public List<ResultItem> getResultItems() {
        return resultItems;
    }

    public void setResultItems(List<ResultItem> resultItems) {
        this.resultItems = resultItems;
    }
}
