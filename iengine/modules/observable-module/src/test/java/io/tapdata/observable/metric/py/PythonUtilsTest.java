package io.tapdata.observable.metric.py;

import io.tapdata.entity.logger.TapLog;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PythonUtilsTest {

    @Nested
    class NeedSkipTest {
         @Test
         void testNeedSkipNormalNormal() {
             File f = mock(File.class);
             when(f.exists()).thenReturn(true);
             when(f.isFile()).thenReturn(true);
             when(f.getName()).thenReturn("setup.py");
             boolean needSkip = PythonUtils.needSkip(f);
             Assertions.assertFalse(needSkip);
         }
         @Test
         void testNeedSkipNormalNotExist() {
             File f = mock(File.class);
             when(f.exists()).thenReturn(false);
             when(f.isFile()).thenReturn(true);
             when(f.getName()).thenReturn("setup.py");
             boolean needSkip = PythonUtils.needSkip(f);
             Assertions.assertTrue(needSkip);
         }
         @Test
         void testNeedSkipNormalNotFile() {
             File f = mock(File.class);
             when(f.exists()).thenReturn(true);
             when(f.isFile()).thenReturn(false);
             when(f.getName()).thenReturn("setup.py");
             boolean needSkip = PythonUtils.needSkip(f);
             Assertions.assertTrue(needSkip);
         }
         @Test
         void testNeedSkipNormalNotName() {
             File f = mock(File.class);
             when(f.exists()).thenReturn(true);
             when(f.isFile()).thenReturn(true);
             when(f.getName()).thenReturn("install.py");
             boolean needSkip = PythonUtils.needSkip(f);
             Assertions.assertTrue(needSkip);
         }
         @Test
         void testNeedSkipNormalNullFile() {
             boolean needSkip = PythonUtils.needSkip(null);
             Assertions.assertTrue(needSkip);
         }
    }


    @Test
    public void testConcat() {
        String path = "/user/tip";
        String p = PythonUtils.concat(path, "s", "s1");
        Assertions.assertEquals("/user/tip/s/s1", p);
    }

    @Test
    public void testDeleteFileOfDirectory() {
        File file = new File("temp");
        if (file.mkdir() && file.exists()) {
            PythonUtils.deleteFile(file, new TapLog());
        }
        Assertions.assertFalse(file.exists());
    }

    @Test
    public void testDeleteFileOfFile() {
        File file = new File("temp.txt");
        try {
            if (file.createNewFile() && file.exists()) {
                PythonUtils.deleteFile(file, new TapLog());
                Assertions.assertFalse(file.exists());
            }
        } catch (IOException ignore) { }
    }

    @Test
    public void testGetPythonConfig() {
        File file = new File("temp.json");
        createFile(file);
        try {
            Map<String, Object> pythonConfig = PythonUtils.getPythonConfig(file);
            Assertions.assertEquals(2, pythonConfig.size());
            Assertions.assertTrue(pythonConfig.containsKey("key1"));
            Assertions.assertTrue(pythonConfig.containsKey("key2"));
            Assertions.assertEquals("name", pythonConfig.get("key1"));
            Assertions.assertEquals("id", pythonConfig.get("key2"));
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
                    Assertions.assertTrue(fileTargetPath.exists());
                    Assertions.assertTrue(fileTargetPath.isDirectory());
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
                    Assertions.assertTrue(fileTarget.exists());
                    Assertions.assertTrue(fileTarget.isFile());
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
