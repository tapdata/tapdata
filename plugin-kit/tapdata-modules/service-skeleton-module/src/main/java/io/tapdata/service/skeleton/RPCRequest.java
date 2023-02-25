package io.tapdata.service.skeleton;
public abstract class RPCRequest extends RPCBase {
    public static final int MAXRETRYTIMES = 1;
    //cache local retry times; don't need to transfer to another side.
    private int retryTimes = 0;

    public RPCRequest(String type) {
        super(type);
    }

    public void retry() {
        retryTimes++;
    }

    public boolean canRetry() {
        return retryTimes < MAXRETRYTIMES;
    }


}
