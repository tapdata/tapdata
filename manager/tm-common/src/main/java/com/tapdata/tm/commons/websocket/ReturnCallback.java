package com.tapdata.tm.commons.websocket;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2022/4/16 下午3:09
 */
public abstract class ReturnCallback<T> {

    private final long startTime;

    public ReturnCallback() {
        this.startTime = System.currentTimeMillis();
    }

    public boolean canDestroy() {
        return System.currentTimeMillis() - this.startTime > 10 * 60 * 1000;
    }

    public abstract void success(T t);

    public abstract void error(String code, String message);

}
