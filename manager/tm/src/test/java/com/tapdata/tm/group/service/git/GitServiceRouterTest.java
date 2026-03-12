package com.tapdata.tm.group.service.git;

import com.tapdata.tm.group.dto.GroupGitInfoDto;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/**
 *
 * @author samuel
 * @Description
 * @create 2026-01-21 11:09
 **/
@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = {
		GitServiceRouter.class,
		GitHubService.class,
})
class GitServiceRouterTest {
	@Autowired
	GitServiceRouter gitServiceRouter;

	@Test
	void should_autowire_all_git_services() {
		GroupGitInfoDto groupGitInfoDto = new GroupGitInfoDto();
		groupGitInfoDto.setRepoUrl("https://github.com/tapdata/tapdata.git");

		GitService gitService = gitServiceRouter.route(groupGitInfoDto);
		assertInstanceOf(GitHubService.class, gitService);
	}
}