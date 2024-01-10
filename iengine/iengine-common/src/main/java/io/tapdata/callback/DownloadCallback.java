package io.tapdata.callback;


import java.io.IOException;

public interface DownloadCallback {
    public void needDownloadPdkFile(boolean flag) throws IOException;

    public void onProgress(long fileSize, long progress) throws IOException;

    public void onFinish(String downloadSpeed) throws IOException;

    public void onError(Exception ex) throws IOException;
}

