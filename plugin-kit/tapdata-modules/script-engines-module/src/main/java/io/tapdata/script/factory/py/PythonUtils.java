package io.tapdata.script.factory.py;

public class PythonUtils {
    public static final String DEFAULT_PY_SCRIPT_START = "import json, random, time, datetime, uuid, types, yaml\n" + //", yaml"
            "import urllib, urllib2, requests\n" + //", requests"
            "import math, hashlib, base64\n" + //# , yaml, requests\n" +
            "def process(record, context):\n";
    public static final String PYTHON_THREAD_PACKAGE_PATH = "py-lib/site-packages";
    public static final String DEFAULT_PY_SCRIPT = DEFAULT_PY_SCRIPT_START + "\treturn record;\n";
}
