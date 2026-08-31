package com.tapdata.tm.group.service.git;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.group.dto.GroupGitInfoDto;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.kohsuke.github.GHIssueState;
import org.kohsuke.github.GHPullRequest;
import org.kohsuke.github.GHRepository;
import org.kohsuke.github.GHTag;
import org.kohsuke.github.GitHub;
import org.kohsuke.github.GitHubBuilder;
import org.kohsuke.github.HttpConnector;
import org.kohsuke.github.extras.ImpatientHttpConnector;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.net.URI;
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

	/** GitHub API 建连超时，固定 30s —— 连不上不该等到读超时那么久 */
	private static final int CONNECT_TIMEOUT_MILLIS = 30_000;

	private static final Pattern REPO_PATTERN =
			Pattern.compile("^(?:https?://|git@)[^/:]+[:/]([^/]+)/([^/]+?)(?:\\.git)?/?$");

	@Override
	public boolean supports(GroupGitInfoDto gitInfoDto) {
		return gitInfoDto != null && StringUtils.isNotBlank(gitInfoDto.getRepoUrl());
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
			GitHub github = createGitHubClient(gitInfoDto.getRepoUrl(), gitInfoDto.getToken());
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
	 * Create a pull request using GitHub API (hub4j).
	 * If an open PR with the same head and base already exists, returns its URL.
	 *
	 * @return URL of the created or existing pull request
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

			GitHub github = createGitHubClient(gitInfoDto.getRepoUrl(), gitInfoDto.getToken());
			GHRepository repository = github.getRepository(owner + "/" + repo);

			String baseBranch = StringUtils.isNotBlank(gitInfoDto.getBranch())
					? gitInfoDto.getBranch() : repository.getDefaultBranch();

			// Check for existing open PR with same head -> base
			String head = owner + ":" + branchName;
			List<GHPullRequest> existingPRs = repository.queryPullRequests()
					.head(head)
					.base(baseBranch)
					.state(GHIssueState.OPEN)
					.list()
					.toList();
			if (existingPRs != null && !existingPRs.isEmpty()) {
				String prUrl = existingPRs.get(0).getHtmlUrl().toString();
				log.info("Found existing open pull request: {}", prUrl);
				return prUrl;
			}

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

	/** 用 long 算再夹回 int：分钟数配大了（>35791）直接乘会溢出成负数，setReadTimeout 会抛 IllegalArgumentException */
	private int readTimeoutMillis() {
		return (int) Math.min(Integer.MAX_VALUE, exportTimeoutMinutes() * 60_000L);
	}

	private String[] parseOwnerAndRepo(String repoUrl) {
		Matcher matcher = REPO_PATTERN.matcher(repoUrl);
		if (!matcher.find()) {
			throw new BizException("Git.RepoUrl.InvalidFormat", repoUrl);
		}
		return new String[]{matcher.group(1), matcher.group(2)};
	}

	private GitHub createGitHubClient(String repoUrl, String token) throws IOException {
		// 默认 connector 没有读超时，listTags / createPullRequest 卡住就是永久卡住
		GitHubBuilder builder = new GitHubBuilder()
				.withConnector(new ImpatientHttpConnector(HttpConnector.DEFAULT,
						CONNECT_TIMEOUT_MILLIS, readTimeoutMillis()));
		String host = extractHost(repoUrl);
		if (host != null
				&& !"github.com".equalsIgnoreCase(host)
				&& !"api.github.com".equalsIgnoreCase(host)) {
			builder = builder.withEndpoint("https://" + host + "/api/v3");
		}
		if (StringUtils.isNotBlank(token)) {
			return builder.withOAuthToken(token).build();
		}
		return builder.build();
	}

	private String extractHost(String repoUrl) {
		if (StringUtils.isBlank(repoUrl)) {
			return null;
		}
		if (repoUrl.startsWith("git@")) {
			int colonIdx = repoUrl.indexOf(':');
			if (colonIdx > 4) {
				return repoUrl.substring(4, colonIdx);
			}
			return null;
		}
		try {
			return URI.create(repoUrl).getHost();
		} catch (IllegalArgumentException e) {
			log.warn("Failed to parse host from repo URL: {}", repoUrl);
			return null;
		}
	}
}
