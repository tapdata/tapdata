package io.tapdata.observable.metric.py;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.io.output.BrokenOutputStream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.EnumSet;
import java.util.zip.GZIPOutputStream;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ZipUtilsTest {
    @Test
    void testZip_BrokenOut() {
        final OutputStream out = new BrokenOutputStream();
        assertThatThrownBy(() -> ZipUtils.zip("outputPath", out, false, false)).isInstanceOf(RuntimeException.class);
    }
    @Test
    void testUnTarZip(){
        String outputFileName = "example.txt.gz";
        try (FileOutputStream fileOutputStream = new FileOutputStream(outputFileName);
             GZIPOutputStream gzipOutputStream = new GZIPOutputStream(new BufferedOutputStream(fileOutputStream));
             TarArchiveOutputStream tarArchiveOutputStream = new TarArchiveOutputStream(gzipOutputStream)) {
            // 添加第一个文件到tar文件
            File file1 = new File("file1.txt");
            file1.createNewFile();
            TarArchiveEntry entry1 = new TarArchiveEntry(file1, "file1.txt");
            tarArchiveOutputStream.putArchiveEntry(entry1);
            try (FileInputStream fileInputStream1 = new FileInputStream(file1)) {
                byte[] buffer = new byte[1024];
                int length;
                while ((length = fileInputStream1.read(buffer)) != -1) {
                    tarArchiveOutputStream.write(buffer, 0, length);
                }
            }
            tarArchiveOutputStream.closeArchiveEntry();

            // 添加第二个文件到tar文件
            File file2 = new File("file2.txt");
            file2.createNewFile();
            TarArchiveEntry entry2 = new TarArchiveEntry(file2, "file2.txt");
            tarArchiveOutputStream.putArchiveEntry(entry2);
            try (FileInputStream fileInputStream2 = new FileInputStream(file2)) {
                byte[] buffer = new byte[1024];
                int length;
                while ((length = fileInputStream2.read(buffer)) != -1) {
                    tarArchiveOutputStream.write(buffer, 0, length);
                }
            }
            tarArchiveOutputStream.closeArchiveEntry();

        } catch (IOException e) {
            e.printStackTrace();
        }
        ZipUtils.unTarZip("example.txt.gz", "outputPath");
        Assertions.assertTrue(Files.exists(Paths.get("example.txt.gz")));
    }

    @AfterAll
     static void afterAllTests() throws IOException {
        Path directoryPath = Paths.get(System.getProperty("user.dir")+"/outputPath");
        System.out.println(directoryPath);
        deleteDirectory(directoryPath);
        Files.delete(Paths.get("file1.txt"));
        Files.delete(Paths.get("file2.txt"));
        Files.delete(Paths.get("example.txt.gz"));
    }
    public static void deleteDirectory(Path directoryPath) throws IOException {
        Files.walkFileTree(directoryPath, EnumSet.noneOf(FileVisitOption.class), Integer.MAX_VALUE, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                // 删除文件
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                // 删除目录
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
            }
        });
    }

}
