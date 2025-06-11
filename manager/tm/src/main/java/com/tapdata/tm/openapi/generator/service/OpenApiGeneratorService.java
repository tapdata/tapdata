package com.tapdata.tm.openapi.generator.service;

import com.tapdata.tm.openapi.generator.config.OpenApiGeneratorProperties;
import com.tapdata.tm.openapi.generator.dto.CodeGenerationRequest;
import com.tapdata.tm.openapi.generator.exception.CodeGenerationException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.InputStreamResource;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import javax.annotation.PostConstruct;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * OpenAPI code generation service
 *
 * @author sam
 * @date 2024/12/19
 */
@Service
@Slf4j
public class OpenApiGeneratorService {

	public static final String DEFAULT_JAVA_TEMPLATE_LIBRARY = "okhttp-gson";
	private final OpenApiGeneratorProperties properties;

	// Cached paths resolved during initialization
	private String resolvedJarPath;
	private String resolvedTemplatePath;

	public OpenApiGeneratorService(OpenApiGeneratorProperties properties) {
		this.properties = properties;
	}

	@PostConstruct
	public void init() {
		log.info("OpenAPI Generator Service initializing with configuration:");
		log.info("  Temp directory: {}", properties.getTemp().getDir());
		log.info("  Java version: {}", properties.getJava().getVersion());

		try {
			// Resolve and cache JAR path during initialization
			this.resolvedJarPath = resolveJarPath();
			log.info("✓ JAR path resolved and cached: {}", this.resolvedJarPath);

			// Resolve and cache template path during initialization
			this.resolvedTemplatePath = resolveTemplatePath();
			log.info("✓ Template path resolved and cached: {}", this.resolvedTemplatePath);

			log.info("OpenAPI Generator Service initialization completed successfully");
		} catch (IOException e) {
			log.error("Failed to initialize OpenAPI Generator Service: {}", e.getMessage(), e);
			throw new RuntimeException("OpenAPI Generator Service initialization failed", e);
		}
	}

	/**
	 * Generate code and return JAR or ZIP file (only supports Java language)
	 */
	public ResponseEntity<InputStreamResource> generateCode(CodeGenerationRequest request) throws Exception {
		log.info("Starting code generation with request parameters: {}", request);

		// Validate language support - only Java is supported
		if (!"java".equalsIgnoreCase(request.getLan())) {
			throw new CodeGenerationException(
					String.format("Unsupported language: %s. Currently only 'java' language is supported.", request.getLan())
			);
		}

		// Validate Java runtime version - requires Java 17+
		validateJavaVersion();

		// Create temporary directory
		String sessionId = UUID.randomUUID().toString();
		Path outputDir = Paths.get(properties.getTemp().getDir(), "openapi-generator", sessionId);
		Files.createDirectories(outputDir);

		try {
			// Execute code generation
			log.info("Generator parameters: {}, output dir: {}", request, outputDir);
			executeGenerator(request, outputDir.toString());

			// Check Maven availability and generate appropriate response
			return generateResponse(request, outputDir);

		} finally {
			// Clean up temporary files - temporarily disabled for debugging
			log.info("Temporary files preserved for debugging at: {}", outputDir);
			cleanupTempDirectory(outputDir);
		}
	}

	/**
	 * Generate response based on Maven availability - JAR if Maven available, ZIP otherwise
	 */
	private ResponseEntity<InputStreamResource> generateResponse(CodeGenerationRequest request, Path outputDir) throws Exception {
		return generateZipResponse(request, outputDir);
	}

	/**
	 * Generate ZIP response for Java language
	 */
	private ResponseEntity<InputStreamResource> generateZipResponse(CodeGenerationRequest request, Path outputDir) throws Exception {
		// Create ZIP file containing the generated source code
		Path zipFile = createZipFile(request, outputDir);

		// Prepare response
		String fileName = String.format("%s-%s-v%s-%s.zip", request.getArtifactId(), request.getLan(), request.getVersion(), DateTimeFormatter.ofPattern("yyyyMMddHHmmss").format(LocalDateTime.now()));
		InputStreamResource resource = new InputStreamResource(Files.newInputStream(zipFile));

		HttpHeaders headers = new HttpHeaders();
		headers.add(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + fileName + "\"");
		headers.add(HttpHeaders.CONTENT_TYPE, "application/zip");
		headers.add(HttpHeaders.CONTENT_LENGTH, String.valueOf(Files.size(zipFile)));

		log.info("Code generation completed, ZIP file size: {} bytes", Files.size(zipFile));

		return ResponseEntity.ok()
				.headers(headers)
				.body(resource);
	}

	/**
	 * Create ZIP file containing the generated source code
	 */
	private Path createZipFile(CodeGenerationRequest request, Path outputDir) throws IOException {
		String fileName = String.format("%s-%s-v%s-%s.zip", request.getArtifactId(), request.getLan(), request.getVersion(), DateTimeFormatter.ofPattern("yyyyMMddHHmmss").format(LocalDateTime.now()));
		Path zipFilePath = outputDir.getParent().resolve(fileName);

		log.info("Creating ZIP file: {}", zipFilePath);

		try (ZipOutputStream zipOut = new ZipOutputStream(new FileOutputStream(zipFilePath.toFile()))) {
			// Add all files from the generated project directory
			addDirectoryToZip(outputDir, outputDir, zipOut);
		}

		log.info("ZIP file created successfully: {} (size: {} bytes)", zipFilePath, Files.size(zipFilePath));
		return zipFilePath;
	}

	/**
	 * Recursively add directory contents to ZIP file
	 */
	private void addDirectoryToZip(Path sourceDir, Path baseDir, ZipOutputStream zipOut) throws IOException {
		try (var pathStream = Files.walk(sourceDir)) {
			pathStream.forEach(sourcePath -> {
				try {
					if (Files.isDirectory(sourcePath)) {
						return; // Skip directories, only add files
					}

					// Calculate relative path from base directory
					Path relativePath = baseDir.relativize(sourcePath);
					String zipEntryName = relativePath.toString().replace('\\', '/');

					// Create ZIP entry
					ZipEntry zipEntry = new ZipEntry(zipEntryName);
					zipOut.putNextEntry(zipEntry);

					// Copy file content to ZIP
					try (FileInputStream fis = new FileInputStream(sourcePath.toFile())) {
						byte[] buffer = new byte[8192];
						int length;
						while ((length = fis.read(buffer)) > 0) {
							zipOut.write(buffer, 0, length);
						}
					}

					zipOut.closeEntry();
					log.debug("Added to ZIP: {}", zipEntryName);

				} catch (IOException e) {
					throw new RuntimeException("Failed to add file to ZIP: " + sourcePath, e);
				}
			});
		}
	}

	/**
	 * Resolve resource path, supports classpath and absolute paths
	 */
	private String resolveResourcePath(String path) throws IOException {
		if (StringUtils.hasText(path) && path.startsWith("classpath:")) {
			String resourcePath = path.substring("classpath:".length());
			Resource resource = new ClassPathResource(resourcePath);
			if (resource.exists()) {
				return resource.getFile().getAbsolutePath();
			} else {
				throw new IOException("Classpath resource not found: " + resourcePath);
			}
		}
		return path;
	}

	/**
	 * Auto-detect and resolve JAR path
	 */
	private String resolveJarPath() throws IOException {
		String configuredPath = properties.getJar().getPath();

		if (!"auto".equals(configuredPath)) {
			return resolveResourcePath(configuredPath);
		}

		log.info("Auto-detecting OpenAPI Generator JAR path...");
		log.info("Current working directory: {}", System.getProperty("user.dir"));

		// Auto-detection logic with multiple possible paths
		String[] devPaths = {
				"tapdata/openapi-generator/openapi-generator-cli.jar",           // From project root
				"openapi-generator/openapi-generator-cli.jar",           // From project root
				"../openapi-generator/openapi-generator-cli.jar",       // From manager/tm
				"../../openapi-generator/openapi-generator-cli.jar",    // From manager/tm/target
				"../../../openapi-generator/openapi-generator-cli.jar", // From manager/tm/target/classes
		};

		// 1. Try development paths
		for (String devPath : devPaths) {
			Path devJarPath = Paths.get(devPath);
			if (Files.exists(devJarPath)) {
				log.info("Using development JAR path: {}", devJarPath.toAbsolutePath());
				return devJarPath.toAbsolutePath().toString();
			} else {
				log.debug("Development path not found: {}", devJarPath.toAbsolutePath());
			}
		}

		// 2. Try production path (etc directory)
		String[] prodPaths = {
				"etc/openapi-generator/openapi-generator-cli.jar",
				"../etc/openapi-generator/openapi-generator-cli.jar",
				"../../etc/openapi-generator/openapi-generator-cli.jar",
		};

		for (String prodPath : prodPaths) {
			Path prodJarPath = Paths.get(prodPath);
			if (Files.exists(prodJarPath)) {
				log.info("Using production JAR path: {}", prodJarPath.toAbsolutePath());
				return prodJarPath.toAbsolutePath().toString();
			} else {
				log.debug("Production path not found: {}", prodJarPath.toAbsolutePath());
			}
		}

		// 3. Try classpath as fallback
		try {
			String classpathPath = resolveResourcePath("classpath:openapi-generator/openapi-generator-cli.jar");
			log.info("Using classpath JAR path: {}", classpathPath);
			return classpathPath;
		} catch (IOException e) {
			log.debug("Classpath JAR not found: {}", e.getMessage());
		}

		// 4. List current directory contents for debugging
		try {
			log.warn("JAR file not found. Current directory contents:");
			try (Stream<Path> pathStream = Files.list(Paths.get("."))) {
				pathStream.limit(20)
						.forEach(path -> log.warn("  - {}", path));
			}
		} catch (IOException e) {
			log.warn("Failed to list current directory: {}", e.getMessage());
		}

		throw new IOException("OpenAPI Generator JAR not found in any of the expected locations. " +
				"Tried development paths: " + String.join(", ", devPaths) +
				"; production paths: " + String.join(", ", prodPaths) +
				"; classpath: openapi-generator/openapi-generator-cli.jar");
	}

	/**
	 * Auto-detect and resolve template path
	 */
	private String resolveTemplatePath() throws IOException {
		String configuredPath = properties.getTemplate().getPath();

		if (!"auto".equals(configuredPath)) {
			return resolveResourcePath(configuredPath);
		}

		// Auto-detection logic
		// 1. Try development path (source code)
		String[] devPath = {
				"tapdata/openapi-generator/templates",
				"openapi-generator/templates"
		};
		for (String path : devPath) {
			Path devTemplatePath = Paths.get(path);
			if (Files.exists(devTemplatePath) && Files.isDirectory(devTemplatePath)) {
				log.info("Using development template path: {}", devTemplatePath.toAbsolutePath());
				return devTemplatePath.toAbsolutePath().toString();
			} else {
				log.debug("Development template path not found: {}", devTemplatePath.toAbsolutePath());
			}
		}

		// 2. Try production path (etc directory)
		String[] prodPath = {
				"etc/openapi-generator/templates",
				"../etc/openapi-generator/templates",
				"../../etc/openapi-generator/templates",
				"../../../etc/openapi-generator/templates",
		};
		for (String path : prodPath) {
			Path prodTemplatePath = Paths.get(path);
			if (Files.exists(prodTemplatePath) && Files.isDirectory(prodTemplatePath)) {
				log.info("Using production template path: {}", prodTemplatePath.toAbsolutePath());
				return prodTemplatePath.toAbsolutePath().toString();
			} else {
				log.debug("Production template path not found: {}", prodTemplatePath.toAbsolutePath());
			}
		}

		// 3. Try classpath as fallback
		try {
			String classpathPath = resolveResourcePath("classpath:openapi-generator/templates");
			log.info("Using classpath template path: {}", classpathPath);
			return classpathPath;
		} catch (IOException e) {
			log.debug("Classpath template not found: {}", e.getMessage());
		}

		throw new IOException("OpenAPI Generator templates not found in any of the expected locations: " +
				String.join(",", devPath) + "; " + String.join(",", prodPath) + ", classpath:openapi-generator/templates");
	}

	/**
	 * Execute OpenAPI Generator
	 */
	private void executeGenerator(CodeGenerationRequest request, String outputDir) throws Exception {
		// Use cached JAR path (resolved during initialization)
		log.debug("Using cached JAR path: {}", this.resolvedJarPath);

		List<String> command = new ArrayList<>();
		command.add("java");
		command.add("-jar");
		command.add(this.resolvedJarPath);
		command.add("generate");
		command.add("-i");
		command.add(request.getOas());
		command.add("-g");
		command.add(request.getLan());
		command.add("-o");
		command.add(outputDir);
		command.add("--invoker-package");
		command.add(request.getPackageName());
		command.add("--api-package");
		command.add(request.getPackageName() + ".api");
		command.add("--model-package");
		command.add(request.getPackageName() + ".model");
		command.add("--artifact-id");
		command.add(request.getArtifactId());
		command.add("--group-id");
		command.add(request.getGroupId());
		command.add("--skip-validate-spec");

		// Add additional properties to ensure JAR generation with Java 17
		command.add("--additional-properties");
		var javaVersion = properties.getJava().getVersion();
		var additionalProps = String.format(
				"generatePom=true,generateApiTests=false,generateModelTests=false,java8=false,dateLibrary=java8,sourceFolder=src/main/java,javaVersion=%d," +
						"artifactVersion=%s",
				javaVersion, request.getVersion()
		);
		command.add(additionalProps);

		// Add template parameters if available
		String languageTemplatePath = this.resolvedTemplatePath + File.separator + request.getLan();
		log.info("Template path from config: {}", properties.getTemplate().getPath());
		log.info("Using cached template path: {}", languageTemplatePath);

		// Check if template directory exists
		Path templatePath = Paths.get(languageTemplatePath);
		if (Files.exists(templatePath)) {
			// List template files for debugging
			try {
				log.info("Template directory contents:");
				try (Stream<Path> pathStream = Files.walk(templatePath, 2)) {
					pathStream.filter(Files::isRegularFile)
							.forEach(file -> log.debug("  - {}", file));
				}
			} catch (IOException e) {
				log.warn("Failed to list template directory contents: {}", e.getMessage());
			}

			command.add("-t");
			command.add(languageTemplatePath);
			command.add("--library");
			command.add(DEFAULT_JAVA_TEMPLATE_LIBRARY);
			log.info("Using custom template path: {}, library: {}", languageTemplatePath, DEFAULT_JAVA_TEMPLATE_LIBRARY);
		} else {
			log.warn("Template path does not exist, using default templates: {}", languageTemplatePath);
		}

		log.info("Executing command: {}", String.join(" ", command));

		ProcessBuilder processBuilder = new ProcessBuilder(command);
		processBuilder.redirectErrorStream(true);

		Process process = processBuilder.start();

		// Read output
		StringBuilder output = new StringBuilder();
		try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
			String line;
			while ((line = reader.readLine()) != null) {
				output.append(line).append("\n");
				log.info("Generator output: {}", line);
			}
		}

		int exitCode = process.waitFor();
		if (exitCode != 0) {
			log.error("Code generation failed, exit code: {}, output: {}", exitCode, output);
			throw new CodeGenerationException("Code generation failed: " + output);
		}

		log.info("Code generation successful");

		// Verify that source files were generated
		verifyGeneratedSources(outputDir);

		// Verify that our custom template was used
		verifyCustomTemplate(outputDir);
	}

	/**
	 * Verify that source files were generated by OpenAPI Generator
	 */
	private void verifyGeneratedSources(String outputDir) throws IOException {
		Path outputPath = Paths.get(outputDir);
		Path srcMainJava = outputPath.resolve("src/main/java");

		log.info("Verifying generated sources in: {}", srcMainJava);

		if (!Files.exists(srcMainJava)) {
			throw new CodeGenerationException("Source directory not found: " + srcMainJava);
		}

		// Count Java files
		try (var pathStream = Files.walk(srcMainJava)) {
			long javaFileCount = pathStream
					.filter(path -> path.toString().endsWith(".java"))
					.count();

			log.info("Found {} Java source files", javaFileCount);

			if (javaFileCount == 0) {
				throw new CodeGenerationException("No Java source files were generated");
			}
		}

		// Check for pom.xml
		Path pomFile = outputPath.resolve("pom.xml");
		if (!Files.exists(pomFile)) {
			throw new CodeGenerationException("pom.xml not found in generated project");
		}

		log.info("Source verification completed successfully");
	}

	/**
	 * Verify that our custom template was used by checking pom.xml content
	 */
	private void verifyCustomTemplate(String outputDir) throws IOException {
		Path pomFile = Paths.get(outputDir, "pom.xml");
		if (!Files.exists(pomFile)) {
			log.warn("pom.xml not found, cannot verify custom template usage");
			return;
		}

		String pomContent = Files.readString(pomFile);
		log.info("Checking pom.xml for custom template indicators...");

		// Check for Maven Shade Plugin (our custom template indicator)
		if (pomContent.contains("maven-shade-plugin")) {
			log.info("✓ Custom template detected: maven-shade-plugin found in pom.xml");
		} else {
			log.warn("✗ Custom template NOT detected: maven-shade-plugin not found in pom.xml");
		}

		// Check for finalName configuration
		if (pomContent.contains("<finalName>{{artifactId}}</finalName>") ||
				pomContent.contains("<finalName>" + pomContent.substring(pomContent.indexOf("<artifactId>") + 12, pomContent.indexOf("</artifactId>")) + "</finalName>")) {
			log.info("✓ Custom template detected: finalName configuration found");
		} else {
			log.warn("✗ Custom template NOT detected: finalName configuration not found");
		}

		// Check Java version
		if (pomContent.contains("<source>11</source>") || pomContent.contains("<target>11</target>")) {
			log.info("✓ Custom template detected: Java 11 configuration found");
		} else {
			log.warn("✗ Custom template NOT detected: expected Java version configuration not found");
		}

		// Log first few lines of pom.xml for debugging
		String[] lines = pomContent.split("\n");
		log.info("pom.xml content preview (first 15 lines):");
		for (int i = 0; i < Math.min(15, lines.length); i++) {
			log.info("  {}: {}", i + 1, lines[i]);
		}
	}

	/**
	 * Validate Java runtime version - requires Java 11+
	 * Checks both runtime version and command line java version
	 */
	private void validateJavaVersion() {
		// Check runtime Java version
		String runtimeJavaVersion = System.getProperty("java.version");
		log.debug("Runtime Java version: {}", runtimeJavaVersion);

		try {
			// Parse runtime major version
			int runtimeMajorVersion = parseJavaMajorVersion(runtimeJavaVersion);

			if (runtimeMajorVersion < 11) {
				String errorMessage = String.format(
						"Runtime Java version %s is not supported. Minimum required version is Java 11. Current runtime version: %s",
						runtimeMajorVersion, runtimeJavaVersion
				);
				log.error(errorMessage);
				throw new CodeGenerationException(errorMessage);
			}

			log.info("Runtime Java version validation passed. Current version: {} (major: {})", runtimeJavaVersion, runtimeMajorVersion);

		} catch (NumberFormatException e) {
			String errorMessage = String.format(
					"Unable to parse runtime Java version: %s. Please ensure you are running on Java 11 or higher.",
					runtimeJavaVersion
			);
			log.error(errorMessage, e);
			throw new CodeGenerationException(errorMessage);
		}

		// Check command line Java version
		validateCommandLineJavaVersion();
	}

	/**
	 * Validate command line Java version by executing 'java -version'
	 */
	private void validateCommandLineJavaVersion() {
		try {
			log.debug("Checking command line Java version using 'java -version'");

			ProcessBuilder processBuilder = new ProcessBuilder("java", "-version");
			processBuilder.redirectErrorStream(true);

			Process process = processBuilder.start();

			StringBuilder output = new StringBuilder();
			try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
				String line;
				while ((line = reader.readLine()) != null) {
					output.append(line).append("\n");
				}
			}

			int exitCode = process.waitFor();
			if (exitCode != 0) {
				String errorMessage = "Failed to execute 'java -version' command. Please ensure Java is properly installed and available in PATH.";
				log.error(errorMessage);
				throw new CodeGenerationException(errorMessage);
			}

			String versionOutput = output.toString();
			log.debug("Command line java -version output: {}", versionOutput);

			// Parse version from output
			String commandLineVersion = extractVersionFromJavaVersionOutput(versionOutput);
			int commandLineMajorVersion = parseJavaMajorVersion(commandLineVersion);

			if (commandLineMajorVersion < 11) {
				String errorMessage = String.format(
						"Command line Java version %s is not supported. Minimum required version is Java 11. Current command line version: %s",
						commandLineMajorVersion, commandLineVersion
				);
				log.error(errorMessage);
				throw new CodeGenerationException(errorMessage);
			}

			log.info("Command line Java version validation passed. Version: {} (major: {})", commandLineVersion, commandLineMajorVersion);

		} catch (IOException | InterruptedException e) {
			String errorMessage = "Failed to check command line Java version: " + e.getMessage();
			log.error(errorMessage, e);
			throw new CodeGenerationException(errorMessage);
		}
	}

	/**
	 * Extract version string from 'java -version' command output
	 */
	private String extractVersionFromJavaVersionOutput(String output) {
		// java -version output format examples:
		// Java 8: java version "1.8.0_xxx"
		// Java 11+: java version "11.0.x" or openjdk version "11.0.x"

		String[] lines = output.split("\n");
		for (String line : lines) {
			line = line.trim();
			if (line.contains("version")) {
				// Find version string in quotes
				int startQuote = line.indexOf('"');
				int endQuote = line.lastIndexOf('"');
				if (startQuote != -1 && endQuote != -1 && startQuote < endQuote) {
					return line.substring(startQuote + 1, endQuote);
				}
			}
		}

		throw new NumberFormatException("Unable to extract version from java -version output: " + output);
	}

	/**
	 * Parse major version number from Java version string
	 * Handles both old format (1.8.0_xxx) and new format (11.0.x, 17.0.x)
	 */
	private int parseJavaMajorVersion(String javaVersion) {
		if (javaVersion == null || javaVersion.trim().isEmpty()) {
			throw new NumberFormatException("Java version is null or empty");
		}

		// Remove any leading/trailing whitespace
		javaVersion = javaVersion.trim();

		// Handle old format like "1.8.0_xxx" -> major version is 8
		if (javaVersion.startsWith("1.")) {
			String[] parts = javaVersion.split("\\.");
			if (parts.length >= 2) {
				return Integer.parseInt(parts[1]);
			}
		}

		// Handle new format like "11.0.x", "17.0.x" -> major version is the first number
		String[] parts = javaVersion.split("\\.");
		if (parts.length > 0) {
			// Extract only the numeric part (remove any non-numeric suffixes)
			String majorVersionStr = parts[0].replaceAll("[^0-9]", "");
			if (!majorVersionStr.isEmpty()) {
				return Integer.parseInt(majorVersionStr);
			}
		}

		throw new NumberFormatException("Unable to parse major version from: " + javaVersion);
	}

	/**
	 * Clean up temporary directory
	 */
	private void cleanupTempDirectory(Path tempDir) {
		try {
			if (Files.exists(tempDir)) {
				try (var pathStream = Files.walk(tempDir)) {
					pathStream
							.sorted((a, b) -> b.compareTo(a)) // Delete files first, then directories
							.forEach(path -> {
								try {
									Files.deleteIfExists(path);
								} catch (IOException e) {
									log.warn("Failed to delete temporary file: {}", path, e);
								}
							});
				}
			}
		} catch (IOException e) {
			log.warn("Failed to clean up temporary directory: {}", tempDir, e);
		}
	}
}
