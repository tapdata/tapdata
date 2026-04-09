package com.tapdata.tm.group.service.git;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.group.dto.GroupGitInfoDto;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.kohsuke.github.GHPullRequest;
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
			String[] ownerAndRepo = parseOwnerAndRepo(gitInfoDto.getRepoUrl());
			GitHub github = createGitHubClient(gitInfoDto.getToken());
			GHRepository repository = github.getRepository(ownerAndRepo[0] + "/" + ownerAndRepo[1]);
			List<GitTag> gitTags = new ArrayList<>();
			for (GHTag ghTag : repository.listTags()) {
				GitTag gitTag = new GitTag();
				gitTag.setTag(ghTag.getName());
				gitTag.setCommitSha(ghTag.getCommit().getSHA1());
				try {
					gitTag.setCreateTimestamp(ghTag.getCommit().getCommitDate().getTime());
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
	 * Create a pull request using GitHub API (hub4j)
	 *
	 * @return URL of the created pull request
	 */
	@Override
	public String createPullRequest(GroupGitInfoDto gitInfoDto, String branchName,
			String prTitle, String prDescription) {
		if (gitInfoDto == null || StringUtils.isBlank(gitInfoDto.getRepoUrl())) {
			throw new BizException("Git.RepoUrl.Required");
		}
		if (StringUtils.isBlank(branchName)) {
			throw new BizException("Git.BranchName.Required");
		}

		try {
			String[] ownerAndRepo = parseOwnerAndRepo(gitInfoDto.getRepoUrl());
			String owner = ownerAndRepo[0];
			String repo = ownerAndRepo[1];

			GitHub github = createGitHubClient(gitInfoDto.getToken());
			GHRepository repository = github.getRepository(owner + "/" + repo);

			String baseBranch = StringUtils.isNotBlank(gitInfoDto.getBranch())
					? gitInfoDto.getBranch() : repository.getDefaultBranch();
			String title = StringUtils.isNotBlank(prTitle) ? prTitle
					: "Export: " + branchName;
			String body = StringUtils.isNotBlank(prDescription) ? prDescription : "";

			GHPullRequest pullRequest = repository.createPullRequest(title, branchName, baseBranch, body);
			String prUrl = pullRequest.getHtmlUrl().toString();
			log.info("Created pull request: {}", prUrl);
			return prUrl;
		} catch (IOException e) {
			log.error("Failed to create pull request on GitHub", e);
			throw new BizException("Git.PullRequest.Failed", e, e.getMessage());
		}
	}

	private String[] parseOwnerAndRepo(String repoUrl) {
		Matcher matcher = REPO_PATTERN.matcher(repoUrl);
		if (!matcher.find()) {
			throw new BizException("Git.RepoUrl.InvalidFormat", repoUrl);
		}
		String owner = matcher.group(1);
		String repo = matcher.group(2);
		if (repo.endsWith(".git")) {
			repo = repo.substring(0, repo.length() - 4);
		}
		return new String[]{owner, repo};
	}

	private GitHub createGitHubClient(String token) throws IOException {
		if (StringUtils.isNotBlank(token)) {
			return new GitHubBuilder().withOAuthToken(token).build();
		} else {
			return GitHub.connectAnonymously();
		}
	}
}
