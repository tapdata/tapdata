package com.tapdata.tm.openapi.generator.util;

import lombok.extern.slf4j.Slf4j;
import org.springframework.util.StringUtils;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Maven packaging utility for building JAR files from generated source code
 *
 * @author sam
 * @date 2024/12/19
 */
@Slf4j
public class MavenPackagingUtil {

    private static final String MVNW_SCRIPT = "mvnw";
    private static final String MVNW_CMD = "mvnw.cmd";
    private static final String MAVEN_COMMAND = "mvn";

    /**
     * Package source code into JAR using Maven
     *
     * @param sourceDir Source directory containing pom.xml and source code
     * @return PackagingResult containing success status, JAR path, and error message
     */
    public static PackagingResult packageToJar(Path sourceDir) {
        log.info("Starting Maven packaging for source directory: {}", sourceDir);

        try {
            // Validate source directory
            if (!Files.exists(sourceDir) || !Files.isDirectory(sourceDir)) {
                return PackagingResult.failure("Source directory does not exist or is not a directory: " + sourceDir);
            }

            // Check for pom.xml
            Path pomFile = sourceDir.resolve("pom.xml");
            if (!Files.exists(pomFile)) {
                return PackagingResult.failure("pom.xml not found in source directory: " + sourceDir);
            }

            // Determine Maven command to use
            String mavenCommand = determineMavenCommand(sourceDir);
            log.info("Using Maven command: {}", mavenCommand);

            // Execute Maven package command
            List<String> command = buildMavenCommand(mavenCommand, sourceDir);
            log.info("Executing Maven command: {}", String.join(" ", command));

            ProcessBuilder processBuilder = new ProcessBuilder(command);
            processBuilder.directory(sourceDir.toFile());
            processBuilder.redirectErrorStream(true);

            Process process = processBuilder.start();

            // Capture output
            StringBuilder output = new StringBuilder();
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    output.append(line).append("\n");
                    log.debug("Maven output: {}", line);
                }
            }

            int exitCode = process.waitFor();
            String outputString = output.toString();

            if (exitCode == 0) {
                // Find generated JAR file
                Path jarFile = findGeneratedJar(sourceDir);
                if (jarFile != null && Files.exists(jarFile)) {
                    log.info("Maven packaging successful, JAR file: {}", jarFile);
                    return PackagingResult.success(jarFile, outputString);
                } else {
                    return PackagingResult.failure("Maven build succeeded but JAR file not found in target directory");
                }
            } else {
                log.error("Maven packaging failed with exit code: {}, output: {}", exitCode, outputString);
                return PackagingResult.failure("Maven build failed with exit code " + exitCode + ": " + outputString);
            }

        } catch (Exception e) {
            log.error("Exception during Maven packaging", e);
            return PackagingResult.failure("Exception during Maven packaging: " + e.getMessage());
        }
    }

    /**
     * Determine which Maven command to use (mvnw, mvnw.cmd, or mvn)
     */
    private static String determineMavenCommand(Path sourceDir) {
        // First try to use mvnw from resources directory
        String[] resourceMvnwPaths = {
                "resources/mvnw",
                "../resources/mvnw",
                "../../resources/mvnw",
                "../../../resources/mvnw"
        };

        for (String resourcePath : resourceMvnwPaths) {
            Path mvnwPath = Paths.get(resourcePath);
            if (Files.exists(mvnwPath) && Files.isExecutable(mvnwPath)) {
                log.info("Using mvnw from resources: {}", mvnwPath.toAbsolutePath());
                return mvnwPath.toAbsolutePath().toString();
            }
        }

        // Check for mvnw in source directory
        Path mvnwInSource = sourceDir.resolve(MVNW_SCRIPT);
        if (Files.exists(mvnwInSource)) {
            return "./" + MVNW_SCRIPT;
        }

        // Check for mvnw.cmd on Windows
        Path mvnwCmdInSource = sourceDir.resolve(MVNW_CMD);
        if (Files.exists(mvnwCmdInSource)) {
            return MVNW_CMD;
        }

        // Fall back to system Maven
        return MAVEN_COMMAND;
    }

    /**
     * Build Maven command with appropriate arguments
     */
    private static List<String> buildMavenCommand(String mavenCommand, Path sourceDir) {
        List<String> command = new ArrayList<>();

        // Add Maven command
        command.add(mavenCommand);

        // Add Maven goals and options
        command.add("clean");
        command.add("package");
        command.add("-DskipTests");
        command.add("-Dmaven.javadoc.skip=true");
        command.add("-B"); // Batch mode (non-interactive)
        command.add("-q"); // Quiet mode (less verbose output)

        return command;
    }

    /**
     * Find the generated JAR file in the target directory
     */
    private static Path findGeneratedJar(Path sourceDir) {
        Path targetDir = sourceDir.resolve("target");
        if (!Files.exists(targetDir) || !Files.isDirectory(targetDir)) {
            log.warn("Target directory not found: {}", targetDir);
            return null;
        }

        try {
            // Look for JAR files in target directory
            return Files.walk(targetDir, 1)
                    .filter(path -> path.toString().endsWith(".jar"))
                    .filter(path -> !path.toString().contains("original-"))
                    .filter(path -> !path.toString().contains("sources"))
                    .filter(path -> !path.toString().contains("javadoc"))
                    .findFirst()
                    .orElse(null);
        } catch (IOException e) {
            log.error("Error searching for JAR file in target directory", e);
            return null;
        }
    }

    /**
     * Result of Maven packaging operation
     */
    public static class PackagingResult {
        private final boolean success;
        private final Path jarPath;
        private final String errorMessage;
        private final String output;

        private PackagingResult(boolean success, Path jarPath, String errorMessage, String output) {
            this.success = success;
            this.jarPath = jarPath;
            this.errorMessage = errorMessage;
            this.output = output;
        }

        public static PackagingResult success(Path jarPath, String output) {
            return new PackagingResult(true, jarPath, null, output);
        }

        public static PackagingResult failure(String errorMessage) {
            return new PackagingResult(false, null, errorMessage, null);
        }

        public boolean isSuccess() {
            return success;
        }

        public Path getJarPath() {
            return jarPath;
        }

        public String getErrorMessage() {
            return errorMessage;
        }

        public String getOutput() {
            return output;
        }
    }
}
