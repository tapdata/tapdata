package io.tapdata.flow.engine.V2.node.duckdb.backup;

import com.tapdata.constant.JSONUtil;
import org.apache.commons.codec.digest.DigestUtils;
import org.bson.Document;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

public final class DuckDbBackupFileUtil {

    private DuckDbBackupFileUtil() {
    }

    public static SnapshotFiles copyDuckDbFiles(String dbPath, String dbPathFileName, Document manifest) throws IOException {
        Path sourceDb = Path.of(dbPath);
        if (!Files.exists(sourceDb)) {
            throw new IOException("DuckDB file does not exist: " + dbPath);
        }
        Path snapshotDir = Files.createTempDirectory("tap-duckdb-backup-");
        Path dbDir = Files.createDirectories(snapshotDir.resolve("db"));
        List<Document> files = new ArrayList<>();

        Path copiedDb = copyOneFile(sourceDb, dbDir.resolve(dbPathFileName));
        files.add(fileDocument(copiedDb, "db/" + dbPathFileName, false));

        Path sourceWal = Path.of(dbPath + ".wal");
        if (Files.exists(sourceWal)) {
            String walName = dbPathFileName + ".wal";
            Path copiedWal = copyOneFile(sourceWal, dbDir.resolve(walName));
            files.add(fileDocument(copiedWal, "db/" + walName, true));
        }

        manifest.put("files", files);
        Path manifestPath = snapshotDir.resolve("manifest.json");
        Files.writeString(manifestPath, JSONUtil.obj2JsonPretty(manifest));
        return new SnapshotFiles(snapshotDir, files);
    }

    public static ArchiveFile buildZipArchive(Path snapshotDir, String generationId, boolean compress) throws IOException {
        Path archive = Files.createTempFile("tap-duckdb-backup-" + generationId + "-", ".zip");
        try (OutputStream fileOut = Files.newOutputStream(archive);
             BufferedOutputStream bufferedOut = new BufferedOutputStream(fileOut);
             ZipOutputStream zipOut = new ZipOutputStream(bufferedOut)) {
            if (!compress) {
                zipOut.setLevel(0);
            }
            try (var stream = Files.walk(snapshotDir)) {
                for (Path path : stream.filter(Files::isRegularFile).sorted().toList()) {
                    String entryName = snapshotDir.relativize(path).toString().replace('\\', '/');
                    zipOut.putNextEntry(new ZipEntry(entryName));
                    Files.copy(path, zipOut);
                    zipOut.closeEntry();
                }
            }
        }
        return new ArchiveFile(archive, Files.size(archive), sha256(archive));
    }

    public static void restoreZipArchive(Path archive, String dbPath, Document meta) throws IOException {
        Path extractDir = Files.createTempDirectory("tap-duckdb-restore-");
        try {
            unzip(archive, extractDir);
            verifyExtractedFiles(extractDir, meta);
            Path targetDb = Path.of(dbPath);
            Path parent = targetDb.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            moveExisting(targetDb);
            moveExisting(Path.of(dbPath + ".wal"));

            String dbPathFileName = meta.getString("dbPathFileName");
            Path extractedDb = extractDir.resolve("db").resolve(dbPathFileName);
            if (!Files.exists(extractedDb)) {
                throw new IOException("Backup archive does not contain DuckDB file: " + extractedDb);
            }
            Files.copy(extractedDb, targetDb, StandardCopyOption.REPLACE_EXISTING);

            Path extractedWal = extractDir.resolve("db").resolve(dbPathFileName + ".wal");
            if (Files.exists(extractedWal)) {
                Files.copy(extractedWal, Path.of(dbPath + ".wal"), StandardCopyOption.REPLACE_EXISTING);
            }
        } finally {
            deleteQuietly(extractDir);
        }
    }

    public static String sha256(Path file) throws IOException {
        try (InputStream inputStream = new BufferedInputStream(Files.newInputStream(file))) {
            return DigestUtils.sha256Hex(inputStream);
        }
    }

    public static void deleteQuietly(Path path) {
        if (path == null || !Files.exists(path)) {
            return;
        }
        try (var stream = Files.walk(path)) {
            stream.sorted(Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException ignored) {
                    // best effort cleanup
                }
            });
        } catch (IOException ignored) {
            // best effort cleanup
        }
    }

    @SuppressWarnings("unchecked")
    private static void verifyExtractedFiles(Path extractDir, Document meta) throws IOException {
        Object filesObj = meta.get("files");
        if (!(filesObj instanceof List<?> list)) {
            return;
        }
        for (Object item : list) {
            if (!(item instanceof Map<?, ?> map)) {
                continue;
            }
            Object pathObj = map.get("path");
            Object shaObj = map.get("sha256");
            if (pathObj == null || shaObj == null) {
                continue;
            }
            Path file = extractDir.resolve(String.valueOf(pathObj));
            if (!Files.exists(file)) {
                Boolean optional = map.get("optional") instanceof Boolean bol ? bol : false;
                if (optional) {
                    continue;
                }
                throw new IOException("Backup archive missing file: " + pathObj);
            }
            String actual = sha256(file);
            if (!actual.equals(String.valueOf(shaObj))) {
                throw new IOException("Backup file checksum mismatch: " + pathObj);
            }
        }
    }

    private static Path copyOneFile(Path source, Path target) throws IOException {
        if (!Files.isRegularFile(source)) {
            throw new IOException("DuckDB backup source is not a regular file: " + source);
        }
        Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING);
        return target;
    }

    private static Document fileDocument(Path file, String path, boolean optional) throws IOException {
        return new Document("path", path)
                .append("size", Files.size(file))
                .append("sha256", sha256(file))
                .append("optional", optional);
    }

    private static void unzip(Path archive, Path extractDir) throws IOException {
        try (InputStream fileIn = Files.newInputStream(archive);
             BufferedInputStream bufferedIn = new BufferedInputStream(fileIn);
             ZipInputStream zipIn = new ZipInputStream(bufferedIn)) {
            ZipEntry entry;
            while ((entry = zipIn.getNextEntry()) != null) {
                Path target = extractDir.resolve(entry.getName()).normalize();
                if (!target.startsWith(extractDir)) {
                    throw new IOException("Unsafe zip entry: " + entry.getName());
                }
                if (entry.isDirectory()) {
                    Files.createDirectories(target);
                } else {
                    Path parent = target.getParent();
                    if (parent != null) {
                        Files.createDirectories(parent);
                    }
                    Files.copy(zipIn, target, StandardCopyOption.REPLACE_EXISTING);
                }
                zipIn.closeEntry();
            }
        }
    }

    private static void moveExisting(Path path) throws IOException {
        if (!Files.exists(path)) {
            return;
        }
        Path backup = path.resolveSibling(path.getFileName() + ".restore-bak-" + System.currentTimeMillis());
        Files.move(path, backup, StandardCopyOption.REPLACE_EXISTING);
    }

    public record SnapshotFiles(Path snapshotDir, List<Document> files) {
    }

    public record ArchiveFile(Path path, long size, String sha256) {
    }
}
