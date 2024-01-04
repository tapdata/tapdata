package io.tapdata.callback;

public interface DownloadCallback {
    void needDownloadPdkFile(boolean flag) throws Exception;
    void onProgress(long fileSize,long progress) throws Exception;

    void onFinish(String downloadSpeed) throws Exception;

    void onError(Exception ex) throws Exception;
}
