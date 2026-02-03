package com.tapdata.tm.group.service.git;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.group.dto.GroupGitInfoDto;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.kohsuke.github.GHRepository;
import org.kohsuke.github.GHTag;
import org.kohsuke.github.GitHub;
import org.kohsuke.github.GitHubBuilder;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * GitHub service implementation using hub4j GitHub API
 *
 * @author samuel
 * @Description GitHub-specific operations using hub4j API, inherits JGit operations from base class
 * @create 2026-01-21 10:37
 **/
@Service
@Slf4j
public class GitHubService extends GitBaseService {

	private static final Pattern REPO_PATTERN = Pattern.compile("github\\.com[:/]([^/]+)/([^/.]+)");

	@Override
	public boolean supports(GroupGitInfoDto gitInfoDto) {
		return StringUtils.isNotBlank(gitInfoDto.getRepoUrl())
				&& gitInfoDto.getRepoUrl().contains("github.com");
	}

	/**
	 * List tags using GitHub API (hub4j)
	 */
	@Override
	public List<GitTag> listTags(GroupGitInfoDto gitInfoDto) {
		if (gitInfoDto == null || StringUtils.isBlank(gitInfoDto.getRepoUrl())) {
			throw new BizException("Git.RepoUrl.Required");
		}

		try {
			// Parse owner and repo name from URL
			String[] ownerAndRepo = parseOwnerAndRepo(gitInfoDto.getRepoUrl());
			String owner = ownerAndRepo[0];
			String repo = ownerAndRepo[1];

			// Connect to GitHub API using hub4j
			GitHub github = createGitHubClient(gitInfoDto.getToken());
			GHRepository repository = github.getRepository(owner + "/" + repo);

			// List all tags using GitHub API
			List<GitTag> gitTags = new ArrayList<>();
			for (GHTag ghTag : repository.listTags()) {
				GitTag gitTag = new GitTag();
				gitTag.setTag(ghTag.getName());
				gitTag.setCommitSha(ghTag.getCommit().getSHA1());

				// Get commit date if available
				try {
					long timestamp = ghTag.getCommit().getCommitDate().getTime();
					gitTag.setCreateTimestamp(timestamp);
				} catch (Exception e) {
					log.warn("Failed to get commit date for tag: {}", ghTag.getName(), e);
					gitTag.setCreateTimestamp(0);
				}

				gitTags.add(gitTag);
			}

			return gitTags;
		} catch (IOException e) {
			log.error("Failed to list tags from GitHub API", e);
			throw new BizException("Git.ListTags.Failed", e, e.getMessage());
		}
	}

	/**
	 * Parse owner and repository name from GitHub URL
	 */
	private String[] parseOwnerAndRepo(String repoUrl) {
		Matcher matcher = REPO_PATTERN.matcher(repoUrl);
		if (!matcher.find()) {
			throw new BizException("Git.RepoUrl.InvalidFormat", repoUrl);
		}

		String owner = matcher.group(1);
		String repo = matcher.group(2);

		// Remove .git suffix if present
		if (repo.endsWith(".git")) {
			repo = repo.substring(0, repo.length() - 4);
		}

		return new String[]{owner, repo};
	}

	/**
	 * Create GitHub API client
	 */
	private GitHub createGitHubClient(String token) throws IOException {
		if (StringUtils.isNotBlank(token)) {
			return new GitHubBuilder().withOAuthToken(token).build();
		} else {
			return GitHub.connectAnonymously();
		}
	}
}
