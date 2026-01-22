package com.tapdata.tm.group.service.transfer;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.group.dto.GitOperationStep;
import com.tapdata.tm.group.dto.GroupGitInfoDto;
import com.tapdata.tm.group.dto.GroupInfoDto;
import com.tapdata.tm.group.dto.GroupInfoRecordDto;
import com.tapdata.tm.group.service.git.GitService;
import com.tapdata.tm.group.service.git.GitServiceRouter;
import com.tapdata.tm.group.service.git.GitTag;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.jgit.api.Status;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Component
@Slf4j
public class GitGroupTransferStrategy implements GroupTransferStrategy {

	private static final Pattern REPO_NAME_PATTERN = Pattern.compile("[:/]([^/]+)\\.git$|[:/]([^/]+)$");
	private static final int MAX_RETRY_ATTEMPTS = 5;
	private static final long RETRY_WAIT_MILLIS = 1000;

	private final GitServiceRouter gitServiceRouter;

	public GitGroupTransferStrategy(GitServiceRouter gitServiceRouter) {
		this.gitServiceRouter = gitServiceRouter;
	}

	@Override
	public GroupTransferType getType() {
		return GroupTransferType.GIT;
	}

	@Override
	public void exportGroups(GroupExportRequest request) {
		if (request == null) {
			throw new BizException("Git.Export.Request.Required");
		}

		GroupInfoDto groupInfoDto = request.getGroupInfoDto();
		if (groupInfoDto == null || groupInfoDto.getGitInfo() == null) {
			throw new BizException("Git.Export.GitInfo.Required");
		}

		GroupGitInfoDto gitInfo = groupInfoDto.getGitInfo();
		String repoUrl = gitInfo.getRepoUrl();
		if (StringUtils.isBlank(repoUrl)) {
			throw new BizException("Git.Export.RepoUrl.Required");
		}

		String groupName = groupInfoDto.getName();
		if (StringUtils.isBlank(groupName)) {
			throw new BizException("Git.Export.GroupName.Required");
		}

		File localRepoDir = null;
		try {
			// Get GitService through router
			GitService gitService = recordStep(request, "Get Git Service",
					() -> gitServiceRouter.route(gitInfo), null);

			// Check if tag already exists (if tag is specified)
			String gitTag = request.getGitTag();
			if (StringUtils.isNotBlank(gitTag)) {
				recordStep(request, "Check Tag Existence", () -> {
					List<GitTag> existingTags = gitService.listTags(gitInfo);
					if (existingTags != null) {
						for (GitTag tag : existingTags) {
							if (gitTag.equals(tag.getTag())) {
								throw new BizException("Git.Tag.AlreadyExists", gitTag);
							}
						}
					}
					return null;
				}, "Tag: " + gitTag);
			}

			// Create local temporary directory
			localRepoDir = recordStep(request, "Create Local Temp Directory",
					() -> createLocalTempDirectory(repoUrl), null);

			// Clone repository
			File finalLocalRepoDir = localRepoDir;
			recordStep(request, "Clone Repository", () -> {
				gitService.cloneRepo(gitInfo, finalLocalRepoDir.getAbsolutePath());
				return null;
			}, repoUrl + " -> " + finalLocalRepoDir.getAbsolutePath());

			// Create export subdirectory
			String exportDirName = groupName + "_tapdata_export";
			File exportDir = new File(localRepoDir, exportDirName);
			recordStep(request, "Create Export Directory", () -> {
				if (!exportDir.exists()) {
					if (!exportDir.mkdirs()) {
						throw new BizException("Git.Export.CreateDir.Failed", exportDir.getAbsolutePath());
					}
				}
				return null;
			}, exportDir.getAbsolutePath());

			// Write contents to export directory
			Map<String, byte[]> contents = request.getContents();
			if (contents != null && !contents.isEmpty()) {
				recordStep(request, "Write Files to Directory", () -> {
					writeContentsToDirectory(exportDir, contents);
					return null;
				}, contents.size() + " files");
			}

			// Check working directory status before commit
			Status status = recordStep(request, "Check Git Status",
					() -> gitService.getStatus(finalLocalRepoDir.getAbsolutePath()),
					finalLocalRepoDir.getAbsolutePath(),
				this::buildStatusDetails);

			if (status.isClean()) {
				log.info("No changes to commit for group {}, skipping commit, push and tag operations", groupName);
				// Record the skip step as successful
				recordStep(request, "Skip Commit/Push/Tag", () -> null,
					"No changes detected in working directory, skipping subsequent operations");
			} else {
				// Commit changes with status information
				String commitMessage = buildCommitMessage(groupName, contents, status);
				recordStep(request, "Commit Changes", () -> {
					gitService.commit(gitInfo, finalLocalRepoDir.getAbsolutePath(), commitMessage);
					return null;
				}, "Commit message:\n" + commitMessage);

				// Push changes
				recordStep(request, "Push to Remote", () -> {
					gitService.push(gitInfo, finalLocalRepoDir.getAbsolutePath());
					return null;
				}, repoUrl);

				// Create tag if specified
				if (StringUtils.isNotBlank(gitTag)) {
					recordStep(request, "Create Tag: " + gitTag, () -> {
						gitService.createTag(gitInfo, gitTag, finalLocalRepoDir.getAbsolutePath());
						return null;
					}, "Tag: " + gitTag + " at " + repoUrl);
				}
			}

			log.info("Successfully exported group {} to git repository {}", groupName, repoUrl);
		} catch (Exception e) {
			log.error("Failed to export group to git repository", e);
			if (e instanceof BizException) {
				throw (BizException) e;
			}
			throw new BizException("Git.Export.Failed", e, e.getMessage());
		} finally {
			// Clean up local temporary directory
			if (localRepoDir != null && localRepoDir.exists()) {
				cleanupLocalDirectory(localRepoDir);
			}
		}
	}

	/**
	 * Create local temporary directory for git repository
	 * Retry up to MAX_RETRY_ATTEMPTS times if directory already exists and is not empty
	 */
	private File createLocalTempDirectory(String repoUrl) throws InterruptedException {
		String repoName = parseRepoName(repoUrl);
		String tmpDir = System.getProperty("java.io.tmpdir");

		for (int attempt = 0; attempt < MAX_RETRY_ATTEMPTS; attempt++) {
			long timestamp = System.currentTimeMillis();
			String dirName = repoName + "_" + timestamp;
			File localDir = new File(tmpDir, dirName);

			// Check if directory exists and is not empty
			if (localDir.exists()) {
				String[] files = localDir.list();
				if (files != null && files.length > 0) {
					// Directory exists and is not empty, wait and retry
					log.warn("Directory {} already exists and is not empty, retrying... (attempt {}/{})",
							localDir.getAbsolutePath(), attempt + 1, MAX_RETRY_ATTEMPTS);
					Thread.sleep(RETRY_WAIT_MILLIS);
					continue;
				}
			}

			// Directory doesn't exist or is empty, we can use it
			return localDir;
		}

		throw new BizException("Git.Export.CreateTempDir.Failed",
				"Failed to create temporary directory after " + MAX_RETRY_ATTEMPTS + " attempts");
	}

	/**
	 * Parse repository name from git URL
	 */
	private String parseRepoName(String repoUrl) {
		Matcher matcher = REPO_NAME_PATTERN.matcher(repoUrl);
		if (matcher.find()) {
			// Try first group (with .git), then second group (without .git)
			String name = matcher.group(1);
			if (name == null) {
				name = matcher.group(2);
			}
			if (name != null) {
				return name;
			}
		}

		// Fallback: use last part of URL
		String[] parts = repoUrl.split("[:/]");
		String lastPart = parts[parts.length - 1];
		if (lastPart.endsWith(".git")) {
			lastPart = lastPart.substring(0, lastPart.length() - 4);
		}
		return StringUtils.isNotBlank(lastPart) ? lastPart : "repo";
	}

	/**
	 * Build detailed commit message with git status information
	 */
	private String buildCommitMessage(String groupName, Map<String, byte[]> contents, Status status) {
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String exportDate = dateFormat.format(new Date());

		StringBuilder message = new StringBuilder();
		message.append("Export group: ").append(groupName).append("\n\n");
		message.append("Export date: ").append(exportDate).append("\n");

		if (contents != null && !contents.isEmpty()) {
			message.append("Files exported: ").append(contents.size()).append("\n");
			message.append("File list:\n");
			for (String fileName : contents.keySet()) {
				message.append("  - ").append(fileName).append("\n");
			}
		}

		// Add git status information
		if (status != null) {
			message.append("\nChanges:\n");

			if (!status.getAdded().isEmpty()) {
				message.append("  Added files (").append(status.getAdded().size()).append("):\n");
				for (String file : status.getAdded()) {
					message.append("    + ").append(file).append("\n");
				}
			}

			if (!status.getModified().isEmpty()) {
				message.append("  Modified files (").append(status.getModified().size()).append("):\n");
				for (String file : status.getModified()) {
					message.append("    M ").append(file).append("\n");
				}
			}

			if (!status.getRemoved().isEmpty()) {
				message.append("  Removed files (").append(status.getRemoved().size()).append("):\n");
				for (String file : status.getRemoved()) {
					message.append("    - ").append(file).append("\n");
				}
			}

			if (!status.getUntracked().isEmpty()) {
				message.append("  Untracked files (").append(status.getUntracked().size()).append("):\n");
				for (String file : status.getUntracked()) {
					message.append("    ? ").append(file).append("\n");
				}
			}
		}

		return message.toString();
	}

	/**
	 * Write contents to export directory
	 * If file already exists, it will be overwritten
	 */
	private void writeContentsToDirectory(File exportDir, Map<String, byte[]> contents) throws IOException {
		for (Map.Entry<String, byte[]> entry : contents.entrySet()) {
			String fileName = entry.getKey();
			byte[] fileContent = entry.getValue();

			File file = new File(exportDir, fileName);

			// Delete existing file if present to ensure overwrite
			if (file.exists()) {
				if (!file.delete()) {
					log.warn("Failed to delete existing file {}, will attempt to overwrite", fileName);
				} else {
					log.debug("Deleted existing file {} before writing", fileName);
				}
			}

			Files.write(file.toPath(), fileContent);
			log.debug("Wrote file {} to export directory", fileName);
		}
	}

	/**
	 * Clean up local temporary directory
	 */
	private void cleanupLocalDirectory(File directory) {
		try {
			FileUtils.deleteDirectory(directory);
			log.info("Cleaned up temporary directory: {}", directory.getAbsolutePath());
		} catch (IOException e) {
			log.warn("Failed to clean up temporary directory: {}", directory.getAbsolutePath(), e);
		}
	}

	/**
	 * Functional interface for actions that can throw exceptions
	 */
	@FunctionalInterface
	private interface ThrowingSupplier<T> {
		T get() throws Exception;
	}

	/**
	 * Functional interface for processing result details
	 */
	@FunctionalInterface
	private interface ResultDetailsProcessor<T> {
		String process(T result);
	}

	/**
	 * Record a step in the export process
	 *
	 * @param request     The export request containing the record DTO
	 * @param stepName    The name of the step being executed
	 * @param action      The action to execute
	 * @param detailsInfo Detailed information about the operation (e.g., repository URL, directory path)
	 * @return The result of the action
	 * @throws Exception if the action fails
	 */
	private <T> T recordStep(GroupExportRequest request, String stepName, ThrowingSupplier<T> action, String detailsInfo) throws Exception {
		return recordStep(request, stepName, action, detailsInfo, null);
	}

	/**
	 * Record a step in the export process with result details processor
	 *
	 * @param request              The export request containing the record DTO
	 * @param stepName             The name of the step being executed
	 * @param action               The action to execute
	 * @param detailsInfo          Detailed information about the operation (e.g., repository URL, directory path)
	 * @param resultDetailsProcessor Processor to extract additional details from the result
	 * @return The result of the action
	 * @throws Exception if the action fails
	 */
	private <T> T recordStep(GroupExportRequest request, String stepName, ThrowingSupplier<T> action, String detailsInfo, ResultDetailsProcessor<T> resultDetailsProcessor) throws Exception {
		GroupInfoRecordDto recordDto = request.getRecordDto();
		if (recordDto == null) {
			// No record tracking, just execute the action
			return action.get();
		}

		// Initialize gitOperationSteps list if needed
		if (recordDto.getGitOperationSteps() == null) {
			recordDto.setGitOperationSteps(new ArrayList<>());
		}

		// Create a Git operation step
		GitOperationStep step = new GitOperationStep();
		step.setStepName(stepName);
		step.setStatus(GitOperationStep.StepStatus.IN_PROGRESS);
		step.setTimestamp(new Date());
		long startTime = System.currentTimeMillis();

		try {
			T result = action.get();
			// Mark step as successful
			long duration = System.currentTimeMillis() - startTime;
			step.setStatus(GitOperationStep.StepStatus.SUCCESS);

			// Build detailed success message
			StringBuilder message = new StringBuilder();
			if (StringUtils.isNotBlank(detailsInfo)) {
				message.append(detailsInfo);
			}
			// Add result information if available
			if (result instanceof File file) {
				if (!message.isEmpty()) {
					message.append("\n");
				}
				message.append("Created directory: ").append(file.getAbsolutePath());
			}
			// Process result details if processor is provided
			if (resultDetailsProcessor != null && result != null) {
				String resultDetails = resultDetailsProcessor.process(result);
				if (StringUtils.isNotBlank(resultDetails)) {
					if (!message.isEmpty()) {
						message.append("\n");
					}
					message.append(resultDetails);
				}
			}

			step.setMessage(message.toString());
			step.setDurationMs(duration);
			recordDto.getGitOperationSteps().add(step);
			log.debug("Git export step completed successfully: {} ({}ms)", stepName, duration);
			return result;
		} catch (Exception e) {
			// Mark step as failed
			long duration = System.currentTimeMillis() - startTime;
			step.setStatus(GitOperationStep.StepStatus.FAILED);

			// Build detailed error message
			StringBuilder message = new StringBuilder();
			message.append("Failed: ").append(e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName());
			if (StringUtils.isNotBlank(detailsInfo)) {
				message.append("\n").append(detailsInfo);
			}

			step.setMessage(message.toString());
			step.setDurationMs(duration);
			step.setStackTrace(getStackTrace(e));
			recordDto.getGitOperationSteps().add(step);
			log.error("Git export step failed: {} ({}ms)", stepName, duration, e);
			throw e;
		}
	}

	private String getStackTrace(Exception e) {
		if (null == e) {
			return "";
		}
		StringWriter sw = new StringWriter();
		try (PrintWriter pw = new PrintWriter(sw)) {
			e.printStackTrace(pw);
			return sw.toString();
		}
	}

	/**
	 * Build detailed status information from Git Status
	 *
	 * @param status Git status object
	 * @return Formatted status details string, or empty string if no changes
	 */
	private String buildStatusDetails(Status status) {
		if (status == null) {
			return "";
		}

		StringBuilder details = new StringBuilder();
		boolean hasChanges = false;

		// Added files
		if (!status.getAdded().isEmpty()) {
			hasChanges = true;
			details.append("Added files (").append(status.getAdded().size()).append("):\n");
			for (String file : status.getAdded()) {
				details.append("  + ").append(file).append("\n");
			}
		}

		// Modified files
		if (!status.getModified().isEmpty()) {
			hasChanges = true;
			if (details.length() > 0) {
				details.append("\n");
			}
			details.append("Modified files (").append(status.getModified().size()).append("):\n");
			for (String file : status.getModified()) {
				details.append("  M ").append(file).append("\n");
			}
		}

		// Changed files
		if (!status.getChanged().isEmpty()) {
			hasChanges = true;
			if (details.length() > 0) {
				details.append("\n");
			}
			details.append("Changed files (").append(status.getChanged().size()).append("):\n");
			for (String file : status.getChanged()) {
				details.append("  * ").append(file).append("\n");
			}
		}

		// Removed files
		if (!status.getRemoved().isEmpty()) {
			hasChanges = true;
			if (details.length() > 0) {
				details.append("\n");
			}
			details.append("Removed files (").append(status.getRemoved().size()).append("):\n");
			for (String file : status.getRemoved()) {
				details.append("  - ").append(file).append("\n");
			}
		}

		// Missing files
		if (!status.getMissing().isEmpty()) {
			hasChanges = true;
			if (details.length() > 0) {
				details.append("\n");
			}
			details.append("Missing files (").append(status.getMissing().size()).append("):\n");
			for (String file : status.getMissing()) {
				details.append("  ! ").append(file).append("\n");
			}
		}

		// Untracked files
		if (!status.getUntracked().isEmpty()) {
			hasChanges = true;
			if (details.length() > 0) {
				details.append("\n");
			}
			details.append("Untracked files (").append(status.getUntracked().size()).append("):\n");
			for (String file : status.getUntracked()) {
				details.append("  ? ").append(file).append("\n");
			}
		}

		// Remove trailing newline if exists
		if (hasChanges && details.length() > 0 && details.charAt(details.length() - 1) == '\n') {
			details.setLength(details.length() - 1);
		}

		return hasChanges ? details.toString() : "";
	}
}
