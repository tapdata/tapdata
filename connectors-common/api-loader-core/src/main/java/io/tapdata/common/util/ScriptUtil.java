package io.tapdata.common.util;

import io.tapdata.common.support.APIFactory;

import java.io.*;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.jar.JarFile;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;

public class ScriptUtil {

//    public static void main(String[] args) {
////        ScriptEngineManager engineManager = new ScriptEngineManager();
////        ScriptEngine scriptEngine = engineManager.getEngineByName("nashorn");
////        try {
////            scriptEngine.eval("load('../../js/connector/iengine/connector.js');");
////            Invocable inv = (Invocable) scriptEngine;
////            Object retValue = inv.invokeFunction("discover_schema", null);
////        } catch (ScriptException e) {
////            e.printStackTrace();
////        } catch (NoSuchMethodException e) {
////            e.printStackTrace();
////        }
//        String zhusi = "/**\n" +
//                "*    function aTest(){\n" +
//                "*      var a = 0;\n" +
//                "*    }/\n";
//        String scriptFun1 = "function \n\n" +
//                " discover_schema (connection) {\n" +
//                "    return [\"Pet\", \"User\"];\n" +
//                "}";
//        String scriptFun2 = "await function batch_read (connection,table,size) {\n" +
//                "    return [\"Pet\", \"User\"];\n" +
//                "}";
//        String scriptFun3 = "anysc function stream_read (connection,table,size,consumer) {\n" +
//                "    return [\"Pet\", \"User\"];\n" +
//                "var a = 12;" +
//                "}";
//        String scriptFun4 = "var write_record = async function write_record(connection,table,size,consumer) {\n" +
//                "    return [\"Pet\", \"User\"];\n" +
//                "}";
//        Map<String, String> stringStringMap = keepScriptToMap(zhusi + scriptFun1 + "\n" + scriptFun2 + "\n" + scriptFun3 + "\n" + scriptFun4);
//        stringStringMap.forEach((key, value) -> System.out.println(key));
//    }

    public static final String REGEX1 = "(function[\\s]+)(.*?)([\\s]*\\()(.*?)([\\s|\\n]*\\{)(.*?)(})";
    public static final String REGEX2 = "(function)(\\s+)(.*)([\\s]*)(\\()(.*)([\\s|\n]*)(\\{)(.*)(})";


    public static final String REGEX = "((((var)|(let)|(const))(\\s*)(.*)(\\s*)(=)(\\s*))?function[\\s|\n]*)(.*)";

    public static Map<String, String> keepScriptToMap(String script) {
        Pattern scriptPattern = Pattern.compile(REGEX);
        Matcher matcher = scriptPattern.matcher(script);
        HashMap<String, String> map = new HashMap<>();
        while (matcher.find()) {
            String functionNameJs = matcher.group();
            String functionName = getFunctionName(functionNameJs);
            if (Objects.nonNull(functionName)) {
                map.put(functionName, functionName);
            }
            //System.out.println(functionNameJs);
            //System.out.println(functionName);
            //System.out.println();
        }
        return Collections.unmodifiableMap(map);
    }

    public static final String FUNCTION_REGEX = "";
    public static final String FUNCTION_PRE = "function";
    public static final String FUNCTION_SUF = "(";

    public static String getFunctionName(String function) {
        if (Objects.isNull(function)) return null;

        String[] split = function.split("=");
        if (split.length > 1) {
            String functionName = split[0];
            String[] split1 = functionName.split("\\s");
            if (split1.length > 1) {
                return split1[1].trim();
            } else {
                return split1[0].trim();
            }
        }
        int fromIndex = function.indexOf(FUNCTION_PRE);
        if (fromIndex < 0) return null;
        fromIndex += FUNCTION_PRE.length();
        int endIndex = function.indexOf(FUNCTION_SUF, fromIndex);
        if (endIndex < 0) return null;
        return function.substring(fromIndex, endIndex).trim();
    }


    // var|let|const ... = function ...(){...}
    // (var|let|const)([ ]{1,n})(.*?)([ ]+)(=)([ ]+)(function[^\]{1,n}[ |\\n]+\\{[^\]+})

    // function xxx(){...}
    public static String fileToString(String filePath) throws IOException {
        if (Objects.isNull(filePath)) return null;
        File file = new File(filePath);
        if (!file.exists()) return null;
        return fileToString(new FileInputStream(file));
    }

    public static String fileToString(InputStream connectorJsStream) throws IOException {
        Reader reader = null;
        Writer writer = new StringWriter();
        char[] buffer = new char[1024];
        try {
            reader = new BufferedReader(new InputStreamReader(connectorJsStream, StandardCharsets.UTF_8));
            int n;
            while ((n = reader.read(buffer)) != -1) {
                writer.write(buffer, 0, n);
            }
        } catch (Exception ignored) {
        } finally {
            if (Objects.nonNull(reader)) {
                reader.close();
                writer.close();
                connectorJsStream.close();
            }
        }
        return writer.toString();
    }

    public static InputStream appendStream(InputStream stream, String... appender) {
        List<InputStream> inputStreams = new ArrayList<>();
        inputStreams.add(stream);
        for (String item : appender) {
            inputStreams.add(new ByteArrayInputStream(("\n\n" + item).getBytes(StandardCharsets.UTF_8)));
        }
        return new SequenceInputStream(Collections.enumeration(inputStreams));
    }


    public static String loadFileFromJarPath(String fileName) throws IOException {
        return ScriptUtil.fileToString(ScriptUtil.loadFileStreamFromJarPath(fileName));
    }

    public static InputStream loadFileStreamFromJarPath(String fileName) throws IOException {
        URL resource = APIFactory.class.getClassLoader().getResource(fileName);
        JarFile jarFile = new JarFile(resource.getPath().replace("file:/", "/").replace("!/" + fileName, ""));
        Stream<Map.Entry<ZipEntry, InputStream>> readingStream =
                jarFile.stream().filter(entry -> !entry.isDirectory() && entry.getName().endsWith(fileName))
                        .map(entry -> {
                            try {
                                return new AbstractMap.SimpleEntry<>(entry, jarFile.getInputStream(entry));
                            } catch (IOException e) {
                                return new AbstractMap.SimpleEntry<>(entry, null);
                            }
                        });
        List<Map.Entry<ZipEntry, InputStream>> collect = readingStream.onClose(() -> {
            try {
                jarFile.close();
            } catch (IOException ignored) {
            }
        }).collect(Collectors.toList());
        return collect.get(0).getValue();
    }
}
