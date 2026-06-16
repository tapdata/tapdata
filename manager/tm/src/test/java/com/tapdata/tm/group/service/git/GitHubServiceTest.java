package com.tapdata.tm.group.service.git;

import com.tapdata.tm.group.dto.GroupGitInfoDto;
import org.junit.jupiter.api.Test;
import org.kohsuke.github.GitHub;
import org.springframework.test.util.ReflectionTestUtils;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for GitHubService — covers GHES (GitHub Enterprise Server) support.
 *
 * @author samuel
 * @create 2026-05-28
 */
class GitHubServiceTest {

	private final GitHubService gitHubService = new GitHubService();

	@Test
	void supports_returns_true_for_any_non_blank_repo_url() {
		assertTrue(gitHubService.supports(infoOf("https://github.com/owner/repo.git")));
		assertTrue(gitHubService.supports(infoOf("https://hagithub.home/CHASSIS/dmp-fdm-pas.git")));
		assertTrue(gitHubService.supports(infoOf("https://gitlab.com/owner/repo.git")));
		assertTrue(gitHubService.supports(infoOf("git@hagithub.home:CHASSIS/dmp-fdm-pas.git")));
	}

	@Test
	void supports_returns_false_for_blank_or_null() {
		assertFalse(gitHubService.supports(null));
		assertFalse(gitHubService.supports(infoOf(null)));
		assertFalse(gitHubService.supports(infoOf("")));
		assertFalse(gitHubService.supports(infoOf("   ")));
	}

	@Test
	void parseOwnerAndRepo_handles_ghes_https_with_dot_git() {
		String[] parsed = invokeParse("https://hagithub.home/CHASSIS/dmp-fdm-pas.git");
		assertArrayEquals(new String[]{"CHASSIS", "dmp-fdm-pas"}, parsed);
	}

	@Test
	void parseOwnerAndRepo_handles_github_com_without_dot_git() {
		String[] parsed = invokeParse("https://github.com/CHASSIS/dmp-fdm-pas");
		assertArrayEquals(new String[]{"CHASSIS", "dmp-fdm-pas"}, parsed);
	}

	@Test
	void parseOwnerAndRepo_handles_scp_style_ssh_url() {
		String[] parsed = invokeParse("git@hagithub.home:CHASSIS/dmp-fdm-pas.git");
		assertArrayEquals(new String[]{"CHASSIS", "dmp-fdm-pas"}, parsed);
	}

	@Test
	void createGitHubClient_uses_ghes_endpoint_for_non_github_host() throws Exception {
		GitHub github = invokeCreateClient("https://hagithub.home/CHASSIS/dmp-fdm-pas.git", null);
		assertNotNull(github);
		assertEquals("https://hagithub.home/api/v3", github.getApiUrl());
	}

	@Test
	void createGitHubClient_uses_default_endpoint_for_github_com() throws Exception {
		GitHub github = invokeCreateClient("https://github.com/owner/repo.git", null);
		assertNotNull(github);
		assertEquals("https://api.github.com", github.getApiUrl());
	}

	@Test
	void extractHost_supports_scp_style_url() {
		String host = ReflectionTestUtils.invokeMethod(gitHubService, "extractHost",
				"git@hagithub.home:CHASSIS/dmp-fdm-pas.git");
		assertEquals("hagithub.home", host);
	}

	@Test
	void extractHost_supports_https_url() {
		String host = ReflectionTestUtils.invokeMethod(gitHubService, "extractHost",
				"https://hagithub.home/CHASSIS/dmp-fdm-pas.git");
		assertEquals("hagithub.home", host);
	}

	private GroupGitInfoDto infoOf(String repoUrl) {
		GroupGitInfoDto dto = new GroupGitInfoDto();
		dto.setRepoUrl(repoUrl);
		return dto;
	}

	private String[] invokeParse(String repoUrl) {
		return ReflectionTestUtils.invokeMethod(gitHubService, "parseOwnerAndRepo", repoUrl);
	}

	private GitHub invokeCreateClient(String repoUrl, String token) {
		return ReflectionTestUtils.invokeMethod(gitHubService, "createGitHubClient", repoUrl, token);
	}
}
