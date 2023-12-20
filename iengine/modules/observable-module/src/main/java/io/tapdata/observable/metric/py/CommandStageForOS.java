package io.tapdata.observable.metric.py;

import java.io.File;

interface CommandStageForOS {
    static ProcessBuilder getProcessBuilder(String unPackageAbsolutePath, String pythonJarPath) {
        if (System.getProperty("os.name").toLowerCase().contains("windows")) {
            return new Win10Command().execute(unPackageAbsolutePath, pythonJarPath);
        } else {
            return new LinuxShell().execute(unPackageAbsolutePath, pythonJarPath);
        }
    }

    ProcessBuilder execute(String unPackageAbsolutePath, String pythonJarPath);
}

class Win10Command implements CommandStageForOS {
    protected static final String PACKAGE_COMPILATION_COMMAND = "cd %s; java -jar %s setup.py install";
    @Override
    public ProcessBuilder execute(String unPackageAbsolutePath, String pythonJarPath) {
        return new ProcessBuilder().command("cmd.exe", "/c",
                String.format(PACKAGE_COMPILATION_COMMAND, unPackageAbsolutePath, new File(pythonJarPath).getAbsolutePath()));
    }
}

class LinuxShell implements CommandStageForOS {
    protected static final String PACKAGE_COMPILATION_COMMAND = "cd %s; java -jar %s setup.py install";
    @Override
    public ProcessBuilder execute(String unPackageAbsolutePath, String pythonJarPath) {
        return new ProcessBuilder().command("/bin/sh", "-c",
                String.format(PACKAGE_COMPILATION_COMMAND, unPackageAbsolutePath, new File(pythonJarPath).getAbsolutePath()));
    }
}
