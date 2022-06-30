package io.tapdata.pdk.core.classloader;

import java.io.File;

public interface JarFoundListener {
    /**
     * Check the jar file need reload or not.
     * This method may be call periodically for jar modification detection.
     *
     * @param jarFile
     * @param firstTime
     * @return
     */
    boolean needReloadJar(File jarFile, boolean firstTime);
}
