package io.tapdata.pdk.core.classloader;

import java.io.File;

public interface JarLoadCompletedListener {
    /**
     * Jar load completed no matter successfully or failed
     *
     * @param jarFile
     * @param throwable if failed, throwable is the error information
     * @return
     */
    void loadCompleted(File jarFile, ClassLoader classLoader, Throwable throwable);
}
