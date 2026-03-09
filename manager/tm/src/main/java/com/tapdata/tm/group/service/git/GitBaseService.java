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
import org.eclipse.jgit.transport.RefSpec;
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

		File localDir = new File(localPath);
		if (localDir.exists()) {
			throw new BizException("Git.LocalPath.AlreadyExists", localPath);
		}

		try {
			String repoUrl = gitInfoDto.getRepoUrl();
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
	 * Create and checkout a new local branch using JGit
	 */
	@Override
	public void createBranch(String localPath, String branchName) {
		if (StringUtils.isBlank(localPath)) {
			throw new BizException("Git.LocalPath.Required");
		}
		if (StringUtils.isBlank(branchName)) {
			throw new BizException("Git.BranchName.Required");
		}

		File localDir = new File(localPath);
		if (!localDir.exists() || !localDir.isDirectory()) {
			throw new BizException("Git.LocalPath.NotExists", localPath);
		}

		try (Git git = Git.open(localDir)) {
			git.checkout()
					.setCreateBranch(true)
					.setName(branchName)
					.call();
			log.info("Created and checked out branch {} in {}", branchName, localPath);
		} catch (IOException e) {
			log.error("Failed to open git repository at {}", localPath, e);
			throw new BizException("Git.Repository.OpenFailed", e, localPath);
		} catch (GitAPIException e) {
			log.error("Failed to create branch {}", branchName, e);
			throw new BizException("Git.Branch.Failed", e, e.getMessage());
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
			git.add().addFilepattern(".").call();
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
	 * Push the specified branch to remote using JGit
	 */
	@Override
	public void push(GroupGitInfoDto gitInfoDto, String localPath, String branchName) {
		if (StringUtils.isBlank(localPath)) {
			throw new BizException("Git.LocalPath.Required");
		}
		if (StringUtils.isBlank(branchName)) {
			throw new BizException("Git.BranchName.Required");
		}

		File localDir = new File(localPath);
		if (!localDir.exists() || !localDir.isDirectory()) {
			throw new BizException("Git.LocalPath.NotExists", localPath);
		}

		try (Git git = Git.open(localDir)) {
			log.info("Pushing branch {} from {} to remote", branchName, localPath);
			String refSpec = "refs/heads/" + branchName + ":refs/heads/" + branchName;
			Iterable<PushResult> pushResults = git.push()
					.setRefSpecs(new RefSpec(refSpec))
					.setCredentialsProvider(createCredentialsProvider(gitInfoDto != null ? gitInfoDto.getToken() : null))
					.call();
			checkPushResults(pushResults, localPath);
			log.info("Successfully pushed branch {} from {}", branchName, localPath);
		} catch (IOException e) {
			log.error("Failed to open git repository at {}", localPath, e);
			throw new BizException("Git.Repository.OpenFailed", e, localPath);
		} catch (GitAPIException e) {
			log.error("Failed to push branch {}", branchName, e);
			throw new BizException("Git.Push.Failed", e, e.getMessage());
		}
	}

	/**
	 * Default implementation throws unsupported — subclasses for specific platforms override this.
	 */
	@Override
	public String createPullRequest(GroupGitInfoDto gitInfoDto, String branchName,
			String prTitle, String prDescription) {
		throw new BizException("Git.PullRequest.NotSupported");
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
			Ref tagRef = git.tag().setName(tag).call();
			log.info("Created tag {} at {}", tag, localPath);
			Iterable<PushResult> pushResults = git.push()
					.add(tagRef)
					.setCredentialsProvider(createCredentialsProvider(gitInfoDto != null ? gitInfoDto.getToken() : null))
					.call();
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
	 * List tags — subclasses with platform API support should override this.
	 */
	@Override
	public List<GitTag> listTags(GroupGitInfoDto gitInfoDto) {
		throw new BizException("Git.ListTags.NotSupported");
	}

	/**
	 * Get the latest tag name by sorting tags by creation timestamp
	 */
	public String lastestTagName(GroupGitInfoDto gitInfoDto) {
		List<GitTag> gitTags = listTags(gitInfoDto);
		if (gitTags == null || gitTags.isEmpty()) {
			return "";
		}
		return gitTags.stream()
				.max(Comparator.comparingLong(GitTag::getCreateTimestamp))
				.map(GitTag::getTag)
				.orElse(null);
	}

	/**
	 * Get the status of the working directory
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

	private void checkPushResults(Iterable<PushResult> pushResults, String localPath) {
		for (PushResult pushResult : pushResults) {
			log.debug("Push result messages: {}", pushResult.getMessages());
			for (RemoteRefUpdate update : pushResult.getRemoteUpdates()) {
				RemoteRefUpdate.Status status = update.getStatus();
				log.debug("Remote ref update - Ref: {}, Status: {}, Message: {}",
						update.getRemoteName(), status, update.getMessage());
				if (status != RemoteRefUpdate.Status.OK && status != RemoteRefUpdate.Status.UP_TO_DATE) {
					String errorMessage = String.format(
							"Failed to push ref %s from %s. Status: %s, Message: %s",
							update.getRemoteName(), localPath, status,
							update.getMessage() != null ? update.getMessage() : "No error message");
					log.error(errorMessage);
					throw new BizException("Git.Push.Failed", errorMessage);
				}
			}
		}
	}

	protected CredentialsProvider createCredentialsProvider(String token) {
		if (StringUtils.isNotBlank(token)) {
			log.debug("Creating credentials provider with token authentication");
			return new UsernamePasswordCredentialsProvider("x-access-token", token);
		}
		return null;
	}
}
