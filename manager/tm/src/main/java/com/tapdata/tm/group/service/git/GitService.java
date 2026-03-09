package com.tapdata.tm.group.service.git;

import com.tapdata.tm.group.dto.GroupGitInfoDto;
import org.eclipse.jgit.api.Status;

import java.util.List;

/**
 * Git service interface for repository operations
 *
 * @author samuel
 * @Description Git operations including clone, commit, push, tag, branch and PR management
 * @create 2026-01-21 10:36
 **/
public interface GitService {
	/**
	 * Check if this service supports the given git info
	 *
	 * @param gitInfoDto git repository information
	 * @return true if supported
	 */
	boolean supports(GroupGitInfoDto gitInfoDto);

	/**
	 * Clone repository to local path
	 *
	 * @param gitInfoDto git repository information including URL and token
	 * @param localPath  local directory path to clone into
	 */
	void cloneRepo(GroupGitInfoDto gitInfoDto, String localPath);

	/**
	 * Create and checkout a new local branch
	 *
	 * @param localPath  local repository path
	 * @param branchName new branch name
	 */
	void createBranch(String localPath, String branchName);

	/**
	 * Commit changes in local repository
	 *
	 * @param gitInfoDto git repository information
	 * @param localPath  local repository path
	 * @param message    commit message
	 */
	void commit(GroupGitInfoDto gitInfoDto, String localPath, String message);

	/**
	 * Push local branch to remote repository
	 *
	 * @param gitInfoDto git repository information (contains token for authentication)
	 * @param localPath  local repository path
	 * @param branchName branch name to push
	 */
	void push(GroupGitInfoDto gitInfoDto, String localPath, String branchName);

	/**
	 * Create a pull request from the given branch to the base branch
	 *
	 * @param gitInfoDto  git repository information
	 * @param branchName  source branch name (head)
	 * @param prTitle     PR title
	 * @param prDescription PR description/body
	 * @return URL of the created pull request
	 */
	String createPullRequest(GroupGitInfoDto gitInfoDto, String branchName, String prTitle, String prDescription);

	/**
	 * Create and push a tag
	 *
	 * @param gitInfoDto git repository information
	 * @param tag        tag name
	 * @param localPath  local repository path
	 */
	void createTag(GroupGitInfoDto gitInfoDto, String tag, String localPath);

	/**
	 * List all tags from remote repository
	 *
	 * @param gitInfoDto git repository information
	 * @return list of tags sorted by creation time (newest first)
	 */
	List<GitTag> listTags(GroupGitInfoDto gitInfoDto);

	Status getStatus(String localPath);
}