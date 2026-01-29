package com.tapdata.tm.group.service.git;

import com.tapdata.tm.group.dto.GroupGitInfoDto;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 *
 * @author samuel
 * @Description
 * @create 2026-01-21 10:54
 **/
@Component
public class GitServiceRouter {
	private final List<GitService> gitServices;

	public GitServiceRouter(List<GitService> gitServices) {
		this.gitServices = gitServices;
	}

	public GitService route(GroupGitInfoDto groupGitInfoDto) {
		if (groupGitInfoDto == null) {
			throw new IllegalArgumentException("groupGitInfoDto is null");
		}
		return gitServices.stream()
				.filter(service -> service.supports(groupGitInfoDto))
				.findFirst()
				.orElseThrow(() ->
						new IllegalArgumentException("Unsupported git platform: " + groupGitInfoDto.getRepoUrl()));
	}
}
