package io.tapdata.observable.metric.py;

import io.tapdata.entity.logger.TapLog;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

public class PythonUtilsTest {

    @Test
    public void testConcat() {
        String path = "/user/tip";
        String p = PythonUtils.concat(path, "s", "s1");
        Assert.assertEquals("/user/tip/s/s1", p);
    }

    @Test
    public void testDeleteFileOfDirectory() {
        File file = new File("temp");
        if (file.mkdir() && file.exists()) {
            PythonUtils.deleteFile(file, new TapLog());
        }
        Assert.assertFalse(file.exists());
    }

    @Test
    public void testDeleteFileOfFile() {
        File file = new File("temp.txt");
        try {
            if (file.createNewFile() && file.exists()) {
                PythonUtils.deleteFile(file, new TapLog());
                Assert.assertFalse(file.exists());
            }
        } catch (IOException ignore) { }
    }

    @Test
    public void testGetPythonConfig() {
        File file = new File("temp.json");
        createFile(file);
        try {
            Map<String, Object> pythonConfig = PythonUtils.getPythonConfig(file);
            Assert.assertEquals(2, pythonConfig.size());
            Assert.assertTrue(pythonConfig.containsKey("key1"));
            Assert.assertTrue(pythonConfig.containsKey("key2"));
            Assert.assertEquals("name", pythonConfig.get("key1"));
            Assert.assertEquals("id", pythonConfig.get("key2"));
        } finally {
            PythonUtils.deleteFile(file, new TapLog());
        }
    }

    private void createFile(File file) {
        String map = "{\"key1\": \"name\", \"key2\": \"id\"}";
        try (BufferedWriter out = new BufferedWriter(new FileWriter(file))){
            out.write(map);
        } catch (Exception ignore) { }
    }

    @Test
    public void testCopyFile() {
        File file = new File("temp.json");
        try {
            createFile(file);
            if (file.exists()) {
                File fileTargetPath = new File("temp");
                try {
                    try {
                        PythonUtils.copyFile(file, fileTargetPath);
                    } catch (Exception ignore) {}
                    Assert.assertTrue(fileTargetPath.exists());
                    Assert.assertTrue(fileTargetPath.isDirectory());
                } finally {
                    PythonUtils.deleteFile(fileTargetPath, new TapLog());
                }
            }
        } finally {
            PythonUtils.deleteFile(file, new TapLog());
        }
    }

    @Test
    public void testSaveTempFile() {
        File file = new File("temp.json");
        try {
            createFile(file);
            if (file.exists()) {
                File fileTarget = new File("temp_1.json");
                try (FileInputStream inputStream = new FileInputStream(file)) {
                    try {
                        PythonUtils.saveTempZipFile(inputStream, "temp_1.json", new TapLog());
                    } catch (Exception ignore) {}
                    Assert.assertTrue(fileTarget.exists());
                    Assert.assertTrue(fileTarget.isFile());
                } catch (IOException e) {

                } finally {
                    PythonUtils.deleteFile(fileTarget, new TapLog());
                }
            }
        } finally {
            PythonUtils.deleteFile(file, new TapLog());
        }
    }
}
