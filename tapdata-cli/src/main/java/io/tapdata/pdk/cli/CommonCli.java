package io.tapdata.pdk.cli;

import io.tapdata.entity.error.CoreException;
import io.tapdata.pdk.core.error.PDKRunnerErrorCodes;
import io.tapdata.pdk.core.utils.CommonUtils;

import java.util.concurrent.Callable;

public abstract class CommonCli implements Callable<Integer> {
    public Integer call() throws Exception {
        int javaVersion = CommonUtils.getJavaVersion();
        if (javaVersion < 8) {
            throw new CoreException(PDKRunnerErrorCodes.CLI_JAVA_VERSION_ILLEGAL, "TapData Agent need Java version to be at least 8, actual version is " + javaVersion + ", to ensure the stability of processing data, please install java 8 and start again, process will be exited now.");
        }
        return execute();
    }

    protected String getMavenHome(String mavenHome) {
        if (mavenHome == null)
            mavenHome = CommonUtils.getProperty("MAVEN_HOME", null);
        if (mavenHome == null)
            throw new IllegalArgumentException("Missing MAVEN_HOME or use -m to specified maven home");
        return mavenHome;
    }

    protected abstract Integer execute() throws Exception;

}
