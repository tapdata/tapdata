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

    private static final String MAVEN_COMMAND = "mvn";
    private static final String MAVEN_CMD_COMMAND = "mvn.cmd";

    // Operating system detection
    private static final String OS_NAME = System.getProperty("os.name").toLowerCase();
    private static final boolean IS_WINDOWS = OS_NAME.contains("win") && !OS_NAME.contains("darwin");
    private static final boolean IS_LINUX = OS_NAME.contains("linux") || OS_NAME.contains("nux") ||
                                           OS_NAME.equals("ubuntu") || OS_NAME.contains("centos") ||
                                           OS_NAME.contains("redhat") || OS_NAME.contains("red hat") ||
                                           OS_NAME.contains("suse") || OS_NAME.contains("debian") ||
                                           OS_NAME.contains("fedora") || OS_NAME.contains("arch");
    private static final boolean IS_MAC = OS_NAME.contains("mac") || OS_NAME.contains("darwin");

    /**
     * Get operating system information for logging
     */
    private static String getOSInfo() {
        return String.format("OS: %s (Windows: %s, Linux: %s, Mac: %s)",
                OS_NAME, IS_WINDOWS, IS_LINUX, IS_MAC);
    }

    /**
     * Package source code into JAR using Maven with offline mode attempt first
     *
     * @param sourceDir Source directory containing pom.xml and source code
     * @return PackagingResult containing success status, JAR path, and error message
     */
    public static PackagingResult packageToJar(Path sourceDir) {
        log.info("Starting Maven packaging for source directory: {}", sourceDir);
        log.info("Operating system information: {}", getOSInfo());

        // First attempt: Try offline mode if conditions are met
        log.info("[OFFLINE MODE] Attempting offline Maven packaging first...");
        PackagingResult offlineResult = packageToJarWithOfflineMode(sourceDir);
        if (offlineResult.isSuccess()) {
            log.info("[OFFLINE MODE] Offline Maven packaging completed successfully");
            return offlineResult;
        } else {
            log.warn("[OFFLINE MODE] Offline Maven packaging failed: {}", offlineResult.getErrorMessage());
            log.info("[ONLINE MODE] Falling back to online mode...");
        }

        // Second attempt: Fall back to original online mode
        PackagingResult onlineResult = packageToJarOnline(sourceDir);
        if (onlineResult.isSuccess()) {
            log.info("[ONLINE MODE] Online Maven packaging completed successfully");
        } else {
            log.error("[ONLINE MODE] Online Maven packaging also failed: {}", onlineResult.getErrorMessage());
        }
        return onlineResult;
    }

    /**
     * Attempt to package using offline mode with specific conditions
     */
    private static PackagingResult packageToJarWithOfflineMode(Path sourceDir) {
        log.info("[OFFLINE MODE] Checking offline mode prerequisites for source directory: {}", sourceDir);

        try {
            // Check if offline mode conditions are met
            Path currentDir = Paths.get(System.getProperty("user.dir"));
            Path sdkDepsPath = currentDir.resolve("../etc/openapi-generator/sdk-deps");
            // Get Maven binary path based on operating system
            Path mavenBinaryPath = getMavenBinaryPath(currentDir);

            log.debug("[OFFLINE MODE] Checking offline mode conditions:");
            log.debug("[OFFLINE MODE] Current directory: {}", currentDir);
            log.debug("[OFFLINE MODE] SDK deps path: {}", sdkDepsPath);
            log.debug("[OFFLINE MODE] Maven binary path: {}", mavenBinaryPath);

            if (!Files.exists(sdkDepsPath) || !Files.isDirectory(sdkDepsPath)) {
                return PackagingResult.failure("SDK dependencies directory not found: " + sdkDepsPath);
            }

            if (!Files.exists(mavenBinaryPath)) {
                return PackagingResult.failure("Maven binary not found: " + mavenBinaryPath);
            }

            // On Windows, we don't check executable bit as .cmd files may not have it set
            if (!IS_WINDOWS && !Files.isExecutable(mavenBinaryPath)) {
                return PackagingResult.failure("Maven binary not executable: " + mavenBinaryPath);
            }

            // Validate source directory and pom.xml
            if (!Files.exists(sourceDir) || !Files.isDirectory(sourceDir)) {
                return PackagingResult.failure("Source directory does not exist or is not a directory: " + sourceDir);
            }

            Path pomFile = sourceDir.resolve("pom.xml");
            if (!Files.exists(pomFile)) {
                return PackagingResult.failure("pom.xml not found in source directory: " + sourceDir);
            }

            // Build offline Maven command with binary
            List<String> command = buildOfflineMavenCommand(mavenBinaryPath.toAbsolutePath().toString(), sdkDepsPath.toAbsolutePath().toString());
            log.info("[OFFLINE MODE] Executing offline Maven command: {}", String.join(" ", command));

            return executeMavenCommand(command, sourceDir, true);

        } catch (Exception e) {
            log.error("[OFFLINE MODE] Exception during offline Maven packaging", e);
            return PackagingResult.failure("Exception during offline Maven packaging: " + e.getMessage());
        }
    }

    /**
     * Package using original online mode logic
     */
    private static PackagingResult packageToJarOnline(Path sourceDir) {
        log.info("[ONLINE MODE] Starting online Maven packaging for source directory: {}", sourceDir);

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
            String mavenCommand = determineMavenCommand();
            log.info("Using Maven command: {}", mavenCommand);

            // Execute Maven package command
            List<String> command = buildMavenCommand(mavenCommand, sourceDir);
            log.info("[ONLINE MODE] Executing online Maven command: {}", String.join(" ", command));

            return executeMavenCommand(command, sourceDir, false);

        } catch (Exception e) {
            log.error("[ONLINE MODE] Exception during online Maven packaging", e);
            return PackagingResult.failure("Exception during online Maven packaging: " + e.getMessage());
        }
    }

    /**
     * Find Maven binary with unified search strategy
     * 1. Check multiple relative paths for Maven commands
     * 2. Fall back to system Maven if not found locally
     *
     * @param baseDir Base directory for relative path resolution (null for current directory)
     * @param returnAsPath Whether to return Path object (true) or command string (false)
     * @return Maven binary as Path object or command string, never null
     */
    private static Object findMavenBinaryUnified(Path baseDir, boolean returnAsPath) {
        log.debug("Finding Maven binary for OS: {}", OS_NAME);

        // Use current directory if baseDir is null
        if (baseDir == null) {
            baseDir = Paths.get(System.getProperty("user.dir"));
        }

        // Define search paths in order of preference
        String[] searchPaths = {
            "../lib/maven/bin",
            "../../lib/maven/bin",
            "../../../lib/maven/bin"
        };

        // Get possible Maven binary names based on OS
        String[] mavenBinaryNames = IS_WINDOWS ?
            new String[]{"mvn.cmd", "mvn.bat"} :
            new String[]{"mvn"};

        // Search in local relative paths first
        for (String searchPath : searchPaths) {
            for (String binaryName : mavenBinaryNames) {
                Path mavenPath = baseDir.resolve(searchPath + "/" + binaryName);
                if (Files.exists(mavenPath) && (IS_WINDOWS || Files.isExecutable(mavenPath))) {
                    log.info("Found local Maven binary at: {}", mavenPath.toAbsolutePath());
                    if (returnAsPath) {
                        return mavenPath;
                    } else {
                        return mavenPath.toAbsolutePath().toString();
                    }
                }
            }
        }

        // Fall back to system Maven command
        String systemMavenCommand = IS_WINDOWS ? MAVEN_CMD_COMMAND : MAVEN_COMMAND;
        log.info("Local Maven not found, falling back to system Maven command: {}", systemMavenCommand);

        if (returnAsPath) {
            return Paths.get(systemMavenCommand);
        } else {
            return systemMavenCommand;
        }
    }

    /**
     * Get Maven binary path for offline mode (replacement for getMavenBinaryPath)
     */
    private static Path getMavenBinaryPath(Path currentDir) {
        return (Path) findMavenBinaryUnified(currentDir, true);
    }

    /**
     * Determine Maven command for online mode (replacement for determineMavenCommand)
     */
    private static String determineMavenCommand() {
        return (String) findMavenBinaryUnified(null, false);
    }



    /**
     * Build offline Maven command with local repository
     */
    private static List<String> buildOfflineMavenCommand(String mavenBinaryPath, String localRepoPath) {
        List<String> command = new ArrayList<>();

        // Add Maven binary command
        command.add(mavenBinaryPath);

        // Add Maven goals and options
        command.add("clean");
        command.add("package");
        command.add("-DskipTests");
        command.add("-Dmaven.javadoc.skip=true");
        command.add("-B"); // Batch mode (non-interactive)
        command.add("-q"); // Quiet mode (less verbose output)
        command.add("-o"); // Offline mode
        command.add("-Dmaven.repo.local=" + localRepoPath); // Use local repository

        return command;
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
     * Execute Maven command and return result
     */
    private static PackagingResult executeMavenCommand(List<String> command, Path sourceDir, boolean isOfflineMode) {
        String modeLabel = isOfflineMode ? "[OFFLINE MODE]" : "[ONLINE MODE]";

        try {
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
                    log.debug("{} Maven output: {}", modeLabel, line);
                }
            }

            int exitCode = process.waitFor();
            String outputString = output.toString();

            if (exitCode == 0) {
                // Find generated JAR file
                Path jarFile = findGeneratedJar(sourceDir);
                if (jarFile != null && Files.exists(jarFile)) {
                    log.info("{} Maven packaging successful, JAR file: {}", modeLabel, jarFile);
                    return PackagingResult.success(jarFile, outputString);
                } else {
                    return PackagingResult.failure("Maven build succeeded but JAR file not found in target directory");
                }
            } else {
                log.error("{} Maven packaging failed with exit code: {}, output: {}", modeLabel, exitCode, outputString);
                return PackagingResult.failure("Maven build failed with exit code " + exitCode + ": " + outputString);
            }

        } catch (Exception e) {
            log.error("{} Exception during Maven command execution", modeLabel, e);
            return PackagingResult.failure("Exception during Maven command execution: " + e.getMessage());
        }
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
