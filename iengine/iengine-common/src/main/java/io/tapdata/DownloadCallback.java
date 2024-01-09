package io.tapdata;

public interface DownloadCallback {
    public void needDownloadPdkFile(boolean flag) throws Exception;

    public void onProgress(long fileSize,long progress) throws Exception;

    public void onFinish(String downloadSpeed) throws Exception;
    public void onError(Exception ex) throws Exception;
}
