package io.tapdata.pdk.registration;

import io.tapdata.encryptor.JarEncryptor;
import io.tapdata.pdk.core.tapnode.TapNodeInfo;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import static org.junit.jupiter.api.Assertions.*;

class RegistrationArchiveTest {
    private Path connectorJar() throws Exception {
        Path jar = Files.createTempFile("registration-test-", ".jar");
        Manifest manifest = new Manifest();
        manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
        manifest.getMainAttributes().put(Attributes.Name.IMPLEMENTATION_VENDOR, "io.tapdata");
        manifest.getMainAttributes().put(Attributes.Name.IMPLEMENTATION_VERSION, "1.0");
        manifest.getMainAttributes().putValue("Authentication", "GA");
        manifest.getMainAttributes().putValue("PDK-API-Version", "2.0.0");
        try (JarOutputStream output = new JarOutputStream(Files.newOutputStream(jar), manifest)) {
            String path = MetadataFixtureConnector.class.getName().replace('.', '/') + ".class";
            output.putNextEntry(new JarEntry(path));
            try (InputStream input = getClass().getClassLoader().getResourceAsStream(path)) {
                input.transferTo(output);
            }
            output.closeEntry();
            output.putNextEntry(new JarEntry("annotated-spec.json"));
            output.write("{\"properties\":{\"id\":\"fixture\",\"name\":\"Fixture\"}}".getBytes(StandardCharsets.UTF_8));
            output.closeEntry();
        }
        return jar;
    }

    @Test
    void loadsBothPlainAndEncryptedJarsAndReleasesFiles() throws Exception {
        for (boolean encrypted : new boolean[]{false, true}) {
            Path jar = connectorJar();
            try {
                if (encrypted) JarEncryptor.encryptJar(jar.toString());
                try (RegistrationArchive archive = new RegistrationArchive()) {
                    Collection<TapNodeInfo> nodes = archive.load(jar.toFile());
                    assertEquals(1, nodes.size());
                    TapNodeInfo node = nodes.iterator().next();
                    assertEquals("fixture", node.getTapNodeSpecification().getId());
                    assertEquals(TapNodeInfo.NODE_TYPE_SOURCE, node.getNodeType());
                    assertEquals("1.0", RegistrationMetadata.build(node, path -> {}).getString("version"));
                }
            } finally {
                Files.deleteIfExists(jar);
            }
        }
    }

    @Test
    void propagatesLoaderFailures() {
        try (RegistrationArchive archive = new RegistrationArchive()) {
            assertThrows(RuntimeException.class,
                    () -> archive.load(Path.of("/missing-registration-archive.jar").toFile()));
        }
    }
}
