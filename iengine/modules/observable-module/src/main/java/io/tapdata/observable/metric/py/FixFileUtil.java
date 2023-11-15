package io.tapdata.observable.metric.py;

import java.io.File;

public interface FixFileUtil {
    static File fixFile(File file) {
        if (null == file || (file.exists() && file.isDirectory())) {
            return file;
        }
        String name = file.getName();
        if (name.endsWith(FixZipFile.TAG)) {
            return new FixZipFile().fix(file, name);
        }

        if (name.endsWith(FixTarGzFile.TAG)) {
            return new FixTarGzFile().fix(file, name);
        }

        if (name.endsWith(FixGzFile.TAG)) {
            return new FixGzFile().fix(file, name);
        }

        if (name.endsWith(FixJarFile.TAG)) {
            return new FixJarFile().fix(file, name);
        }
        return null;
    }

    default File fix(File file, String fileName) {
        ZipUtils.unzip(file.getAbsolutePath(), file.getParentFile().getAbsolutePath());
        String afterZipFileName = fileName.substring(0, fileName.lastIndexOf(getTag()));
        return new File(file.getParentFile().getAbsolutePath() + File.separator + afterZipFileName);
    }

    String getTag();
}

class FixZipFile implements FixFileUtil {
    public static final String TAG = ".zip";
    @Override
    public String getTag() {
        return TAG;
    }
}

class FixTarGzFile implements FixFileUtil {
    public static final String TAG = ".tar.gz";
    @Override
    public String getTag() {
        return TAG;
    }
}

class FixGzFile implements FixFileUtil {
    public static final String TAG = ".gz";
    @Override
    public String getTag() {
        return TAG;
    }
}

class FixJarFile implements FixFileUtil {
    public static final String TAG = ".jar";
    @Override
    public String getTag() {
        return TAG;
    }
}
