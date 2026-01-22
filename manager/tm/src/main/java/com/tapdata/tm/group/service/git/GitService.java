package com.tapdata.tm.group.service.git;

import com.tapdata.tm.group.dto.GroupGitInfoDto;
import org.eclipse.jgit.api.Status;

import java.util.List;

/**
 * Git service interface for repository operations
 *
 * @author samuel
 * @Description Git operations including clone, commit, push, tag management
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
	 * @param localPath local directory path to clone into
	 */
	void cloneRepo(GroupGitInfoDto gitInfoDto, String localPath);

	/**
	 * Commit changes in local repository
	 *
	 * @param gitInfoDto git repository information (may contain token for future use)
	 * @param localPath local repository path
	 * @param message commit message
	 */
	void commit(GroupGitInfoDto gitInfoDto, String localPath, String message);

	/**
	 * Push local commits to remote repository
	 *
	 * @param gitInfoDto git repository information (may contain token for authentication)
	 * @param localPath local repository path
	 */
	void push(GroupGitInfoDto gitInfoDto, String localPath);

	/**
	 * Create and push a tag
	 *
	 * @param gitInfoDto git repository information (may contain token for authentication)
	 * @param tag tag name
	 * @param localPath local repository path
	 */
	void createTag(GroupGitInfoDto gitInfoDto, String tag, String localPath);

	/**
	 * List all tags from remote repository
	 *
	 * @param gitInfoDto git repository information including URL and token
	 * @return list of tags sorted by creation time (newest first)
	 */
	List<GitTag> listTags(GroupGitInfoDto gitInfoDto);

	Status getStatus(String localPath);
}
