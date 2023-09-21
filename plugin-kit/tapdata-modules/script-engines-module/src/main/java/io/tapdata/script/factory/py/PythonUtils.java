package io.tapdata.script.factory.py;

import io.tapdata.entity.logger.Log;
import io.tapdata.entity.logger.TapLogger;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicReference;

import static io.tapdata.entity.simplify.TapSimplify.fromJson;
import static io.tapdata.entity.simplify.TapSimplify.toJson;

public class PythonUtils {
    public static final String DEFAULT_PY_SCRIPT_START = "import json, random, time, datetime, uuid, types, yaml\n" +
            "import urllib, urllib2, requests\n" +
            "import math, hashlib, base64\n" +
            "def process(record, context):\n";
    public static final String DEFAULT_PY_SCRIPT = DEFAULT_PY_SCRIPT_START + "\treturn record;\n";

    private static final String PACKAGE_COMPILATION_COMMAND = "cd %s; java -jar %s setup.py install";
    private static final String PACKAGE_COMPILATION_FILE = "setup.py";

    public static final String PYTHON_THREAD_PACKAGE_PATH = "py-lib";
    public static final String PYTHON_THREAD_SITE_PACKAGES_PATH = "site-packages";
    public static final String PYTHON_THREAD_JAR = "jython-standalone-2.7.3.jar";
    public static final String PYTHON_SITE_PACKAGES_VERSION_CONFIG = "install.json";

    public static File getThreadPackagePath(){
        File file = new File(concat(PYTHON_THREAD_PACKAGE_PATH, "Lib", PYTHON_THREAD_SITE_PACKAGES_PATH));
        if (!file.exists() || null == file.list() || file.list().length <= 0) {
            return null;
        }
        return file;
    }

    public static final String TAG = PythonUtils.class.getSimpleName();

    public static String concat(String path, String ...paths){
        for (String s : paths) {
            path = FilenameUtils.concat(path, s);
        }
        return path;
    }

    public static void supportThirdPartyPackageList(File file, Log logger) {
        if (null != file && file.exists()) {
            File[] files = file.listFiles();
            if (null != files && files.length > 0) {
                StringJoiner joiner = new StringJoiner(", ");
                for (File f : files) {
                    String name = f.getName();
                    if (f.isFile() && name.contains("-py2.7.egg")) {
                        joiner.add(name.substring(0, name.lastIndexOf("-py2.7.egg")));
                    }
                }
                if (joiner.length() > 0) {
                    logger.info("The sources of third-party packages supported by Python node are as follows: {}", joiner.toString());
                }
            }
        }
    }
}
