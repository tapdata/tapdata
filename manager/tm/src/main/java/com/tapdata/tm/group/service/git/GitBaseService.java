package com.tapdata.tm.group.service.git;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.group.dto.GroupGitInfoDto;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.Status;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.transport.CredentialsProvider;
import org.eclipse.jgit.transport.PushResult;
import org.eclipse.jgit.transport.RemoteRefUpdate;
import org.eclipse.jgit.transport.UsernamePasswordCredentialsProvider;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;

/**
 * Base Git service implementation using JGit for common Git operations
 *
 * @author samuel
 * @Description Base class providing common Git operations using JGit
 * @create 2026-01-21 10:37
 **/
@Service
@Slf4j
public abstract class GitBaseService implements GitService {

	/**
	 * Get the latest tag name by sorting tags by creation timestamp
	 */
	public String lastestTagName(GroupGitInfoDto gitInfoDto) {
		List<GitTag> gitTags = listTags(gitInfoDto);

		if (gitTags == null || gitTags.isEmpty()) {
			return "";
		}

		// Sort by timestamp descending (newest first) and return the first tag name
		return gitTags.stream()
				.max(Comparator.comparingLong(GitTag::getCreateTimestamp))
				.map(GitTag::getTag)
				.orElse(null);
	}

	/**
	 * Clone repository using JGit
	 */
	@Override
	public void cloneRepo(GroupGitInfoDto gitInfoDto, String localPath) {
		if (gitInfoDto == null || StringUtils.isBlank(gitInfoDto.getRepoUrl())) {
			throw new BizException("Git.RepoUrl.Required");
		}
		if (StringUtils.isBlank(localPath)) {
			throw new BizException("Git.LocalPath.Required");
		}

		// Check if directory already exists
		File localDir = new File(localPath);
		if (localDir.exists()) {
			throw new BizException("Git.LocalPath.AlreadyExists", localPath);
		}

		try {
			String repoUrl = gitInfoDto.getRepoUrl();

			// Clone repository using JGit
			try (Git git = Git.cloneRepository()
					.setURI(repoUrl)
					.setDirectory(localDir)
					.setCredentialsProvider(createCredentialsProvider(gitInfoDto.getToken()))
					.setBranch(gitInfoDto.getBranch())
					.call()) {
				log.info("Successfully cloned repository {} to {}", repoUrl, localPath);
			}
		} catch (GitAPIException e) {
			log.error("Failed to clone repository", e);
			throw new BizException("Git.Clone.Failed", e, e.getMessage());
		}
	}

	/**
	 * Commit changes using JGit
	 */
	@Override
	public void commit(GroupGitInfoDto gitInfoDto, String localPath, String message) {
		if (StringUtils.isBlank(localPath)) {
			throw new BizException("Git.LocalPath.Required");
		}
		if (StringUtils.isBlank(message)) {
			throw new BizException("Git.CommitMessage.Required");
		}

		File localDir = new File(localPath);
		if (!localDir.exists() || !localDir.isDirectory()) {
			throw new BizException("Git.LocalPath.NotExists", localPath);
		}

		try (Git git = Git.open(localDir)) {
			// Add all changes
			git.add()
					.addFilepattern(".")
					.call();

			// Commit changes with author information
			// Use a default author if not configured in git config
			git.commit()
					.setMessage(message)
					.setAuthor("Tapdata Manager", "tapdata@tapdata.io")
					.setCommitter("Tapdata Manager", "tapdata@tapdata.io")
					.call();

			log.info("Successfully committed changes in {}", localPath);
		} catch (IOException e) {
			log.error("Failed to open git repository at {}", localPath, e);
			throw new BizException("Git.Repository.OpenFailed", e, localPath);
		} catch (GitAPIException e) {
			log.error("Failed to commit changes", e);
			throw new BizException("Git.Commit.Failed", e, e.getMessage());
		}
	}

	/**
	 * Push changes using JGit
	 */
	@Override
	public void push(GroupGitInfoDto gitInfoDto, String localPath) {
		if (StringUtils.isBlank(localPath)) {
			throw new BizException("Git.LocalPath.Required");
		}

		File localDir = new File(localPath);
		if (!localDir.exists() || !localDir.isDirectory()) {
			throw new BizException("Git.LocalPath.NotExists", localPath);
		}

		try (Git git = Git.open(localDir)) {
			log.info("Preparing to push changes from {} to remote", localPath);
			log.debug("Repository URL: {}", gitInfoDto != null ? gitInfoDto.getRepoUrl() : "N/A");
			log.debug("Branch: {}", gitInfoDto != null ? gitInfoDto.getBranch() : "N/A");

			// Push to remote
			Iterable<PushResult> pushResults = git.push()
					.setCredentialsProvider(createCredentialsProvider(gitInfoDto != null ? gitInfoDto.getToken() : null))
					.call();

			// Check push results
			checkPushResults(pushResults, localPath);

			log.info("Successfully pushed changes from {}", localPath);
		} catch (IOException e) {
			log.error("Failed to open git repository at {}", localPath, e);
			throw new BizException("Git.Repository.OpenFailed", e, localPath);
		} catch (GitAPIException e) {
			log.error("Failed to push changes", e);
			throw new BizException("Git.Push.Failed", e, e.getMessage());
		}
	}

	/**
	 * Create and push tag using JGit
	 */
	@Override
	public void createTag(GroupGitInfoDto gitInfoDto, String tag, String localPath) {
		if (StringUtils.isBlank(tag)) {
			throw new BizException("Git.Tag.Required");
		}
		if (StringUtils.isBlank(localPath)) {
			throw new BizException("Git.LocalPath.Required");
		}

		File localDir = new File(localPath);
		if (!localDir.exists() || !localDir.isDirectory()) {
			throw new BizException("Git.LocalPath.NotExists", localPath);
		}

		try (Git git = Git.open(localDir)) {
			// Create tag
			Ref tagRef = git.tag()
					.setName(tag)
					.call();

			log.info("Created tag {} at {}", tag, localPath);

			// Push tag to remote
			Iterable<PushResult> pushResults = git.push()
					.add(tagRef)
					.setCredentialsProvider(createCredentialsProvider(gitInfoDto != null ? gitInfoDto.getToken() : null))
					.call();

			// Check push results
			checkPushResults(pushResults, localPath);

			log.info("Successfully pushed tag {} from {}", tag, localPath);
		} catch (IOException e) {
			log.error("Failed to open git repository at {}", localPath, e);
			throw new BizException("Git.Repository.OpenFailed", e, localPath);
		} catch (GitAPIException e) {
			log.error("Failed to create or push tag", e);
			throw new BizException("Git.Tag.Failed", e, e.getMessage());
		}
	}

	/**
	 * Check push results to ensure all refs were pushed successfully
	 * Throws BizException if any ref update failed
	 */
	private void checkPushResults(Iterable<PushResult> pushResults, String localPath) {
		for (PushResult pushResult : pushResults) {
			log.debug("Push result messages: {}", pushResult.getMessages());

			for (RemoteRefUpdate update : pushResult.getRemoteUpdates()) {
				RemoteRefUpdate.Status status = update.getStatus();

				log.debug("Remote ref update - Ref: {}, Status: {}, Message: {}, Reason: {}",
						update.getRemoteName(),
						status,
						update.getMessage(),
						update.getStatus());

				// Check if the update was successful
				// OK and UP_TO_DATE are considered successful statuses
				if (status != RemoteRefUpdate.Status.OK && status != RemoteRefUpdate.Status.UP_TO_DATE) {
					String errorMessage = String.format(
							"Failed to push ref %s from %s. Status: %s, Message: %s",
							update.getRemoteName(),
							localPath,
							status,
							update.getMessage() != null ? update.getMessage() : "No error message"
					);
					log.error(errorMessage);
					log.error("Full push result: {}", pushResult.getMessages());
					throw new BizException("Git.Push.Failed", errorMessage);
				}

				log.debug("Successfully pushed ref {} with status {}", update.getRemoteName(), status);
			}
		}
	}

	/**
	 * Create credentials provider for authentication
	 * Uses token as password with a placeholder username (GitHub recommended approach)
	 *
	 * GitHub supports multiple authentication formats:
	 * 1. username=token, password="" (classic approach)
	 * 2. username="x-access-token", password=token (recommended for PAT)
	 * 3. username="oauth2", password=token (for OAuth tokens)
	 */
	protected CredentialsProvider createCredentialsProvider(String token) {
		if (StringUtils.isNotBlank(token)) {
			// Use "x-access-token" as username and token as password
			// This is the recommended approach for GitHub Personal Access Tokens
			log.debug("Creating credentials provider with token authentication");
			return new UsernamePasswordCredentialsProvider("x-access-token", token);
		}
		return null;
	}

	/**
	 * Get the status of the working directory
	 * Returns information about modified, added, removed, and untracked files
	 */
	@Override
	public Status getStatus(String localPath) {
		if (StringUtils.isBlank(localPath)) {
			throw new BizException("Git.LocalPath.Required");
		}

		File localDir = new File(localPath);
		if (!localDir.exists() || !localDir.isDirectory()) {
			throw new BizException("Git.LocalPath.NotExists", localPath);
		}

		try (Git git = Git.open(localDir)) {
			// Get status of working directory
			Status status = git.status().call();

			log.debug("Git status for {}: modified={}, added={}, removed={}, untracked={}",
					localPath,
					status.getModified().size(),
					status.getAdded().size(),
					status.getRemoved().size(),
					status.getUntracked().size());

			return status;
		} catch (IOException e) {
			log.error("Failed to open git repository at {}", localPath, e);
			throw new BizException("Git.Repository.OpenFailed", e, localPath);
		} catch (GitAPIException e) {
			log.error("Failed to get git status", e);
			throw new BizException("Git.Status.Failed", e, e.getMessage());
		}
	}
}
