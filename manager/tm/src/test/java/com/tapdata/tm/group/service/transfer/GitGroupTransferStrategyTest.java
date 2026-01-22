package com.tapdata.tm.group.service.transfer;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.group.dto.GitOperationStep;
import com.tapdata.tm.group.dto.GroupGitInfoDto;
import com.tapdata.tm.group.dto.GroupInfoDto;
import com.tapdata.tm.group.dto.GroupInfoRecordDto;
import com.tapdata.tm.group.service.git.GitService;
import com.tapdata.tm.group.service.git.GitServiceRouter;
import com.tapdata.tm.group.service.git.GitTag;
import org.eclipse.jgit.api.Status;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.File;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for GitGroupTransferStrategy
 * Mocks all Git operations to avoid real Git connections
 *
 * @author samuel
 */
@ExtendWith(MockitoExtension.class)
class GitGroupTransferStrategyTest {

    @Mock
    private GitServiceRouter gitServiceRouter;

    @Mock
    private GitService gitService;

    @Mock
    private Status gitStatus;

    @InjectMocks
    private GitGroupTransferStrategy strategy;

    @TempDir
    File tempDir;

    private GroupExportRequest request;
    private GroupInfoDto groupInfoDto;
    private GroupGitInfoDto gitInfo;
    private GroupInfoRecordDto recordDto;

    @BeforeEach
    void setUp() {
        // Setup git info
        gitInfo = new GroupGitInfoDto();
        gitInfo.setRepoUrl("https://github.com/test/test-repo.git");
        gitInfo.setToken("test-token");
        gitInfo.setBranch("main");

        // Setup group info
        groupInfoDto = new GroupInfoDto();
        groupInfoDto.setName("test-group");
        groupInfoDto.setGitInfo(gitInfo);

        // Setup record DTO
        recordDto = new GroupInfoRecordDto();
        recordDto.setGitOperationSteps(new ArrayList<>());

        // Setup export request
        Map<String, byte[]> contents = new HashMap<>();
        contents.put("file1.json", "{\"test\":\"data1\"}".getBytes());
        contents.put("file2.json", "{\"test\":\"data2\"}".getBytes());

        request = new GroupExportRequest(null, contents, "test-export", groupInfoDto, null, recordDto);
    }

    @Test
    void testGetType() {
        assertEquals(GroupTransferType.GIT, strategy.getType());
    }

    @Test
    void testExportGroups_NullRequest() {
        BizException exception = assertThrows(BizException.class, () -> strategy.exportGroups(null));
        assertEquals("Git.Export.Request.Required", exception.getErrorCode());
    }

    @Test
    void testExportGroups_NullGroupInfo() {
        request.setGroupInfoDto(null);
        BizException exception = assertThrows(BizException.class, () -> strategy.exportGroups(request));
        assertEquals("Git.Export.GitInfo.Required", exception.getErrorCode());
    }

    @Test
    void testExportGroups_NullGitInfo() {
        groupInfoDto.setGitInfo(null);
        BizException exception = assertThrows(BizException.class, () -> strategy.exportGroups(request));
        assertEquals("Git.Export.GitInfo.Required", exception.getErrorCode());
    }

    @Test
    void testExportGroups_BlankRepoUrl() {
        gitInfo.setRepoUrl("");
        BizException exception = assertThrows(BizException.class, () -> strategy.exportGroups(request));
        assertEquals("Git.Export.RepoUrl.Required", exception.getErrorCode());
    }

    @Test
    void testExportGroups_BlankGroupName() {
        groupInfoDto.setName("");
        BizException exception = assertThrows(BizException.class, () -> strategy.exportGroups(request));
        assertEquals("Git.Export.GroupName.Required", exception.getErrorCode());
    }

    @Test
    void testExportGroups_TagAlreadyExists() {
        // Setup tag
        request.setGitTag("v1.0.0");

        // Mock git service router
        when(gitServiceRouter.route(any(GroupGitInfoDto.class))).thenReturn(gitService);

        // Mock existing tags
        GitTag existingTag = new GitTag();
        existingTag.setTag("v1.0.0");
        existingTag.setCreateTimestamp(System.currentTimeMillis());
        when(gitService.listTags(any(GroupGitInfoDto.class))).thenReturn(Collections.singletonList(existingTag));

        BizException exception = assertThrows(BizException.class, () -> strategy.exportGroups(request));
        assertEquals("Git.Tag.AlreadyExists", exception.getErrorCode());
    }

    @Test
    void testExportGroups_SuccessWithChanges() throws Exception {
        // Mock git service router
        when(gitServiceRouter.route(any(GroupGitInfoDto.class))).thenReturn(gitService);

        // Mock git operations
        doNothing().when(gitService).cloneRepo(any(GroupGitInfoDto.class), anyString());

        // Mock git status with changes - need to mock all methods called by buildStatusDetails
        when(gitStatus.isClean()).thenReturn(false);
        when(gitStatus.getAdded()).thenReturn(Set.of("file1.json", "file2.json"));
        when(gitStatus.getModified()).thenReturn(Collections.emptySet());
        when(gitStatus.getChanged()).thenReturn(Collections.emptySet());
        when(gitStatus.getRemoved()).thenReturn(Collections.emptySet());
        when(gitStatus.getMissing()).thenReturn(Collections.emptySet());
        when(gitStatus.getUntracked()).thenReturn(Collections.emptySet());
        when(gitService.getStatus(anyString())).thenReturn(gitStatus);

        doNothing().when(gitService).commit(any(GroupGitInfoDto.class), anyString(), anyString());
        doNothing().when(gitService).push(any(GroupGitInfoDto.class), anyString());

        // Execute
        strategy.exportGroups(request);

        // Verify git operations were called
        verify(gitServiceRouter).route(gitInfo);
        verify(gitService).cloneRepo(eq(gitInfo), anyString());
        verify(gitService).getStatus(anyString());
        verify(gitService).commit(eq(gitInfo), anyString(), anyString());
        verify(gitService).push(eq(gitInfo), anyString());

        // Verify operation steps were recorded
        assertNotNull(recordDto.getGitOperationSteps());
        assertTrue(recordDto.getGitOperationSteps().size() > 0);
    }

    @Test
    void testExportGroups_SuccessWithChangesAndTag() throws Exception {
        // Setup tag
        request.setGitTag("v1.0.0");

        // Mock git service router
        when(gitServiceRouter.route(any(GroupGitInfoDto.class))).thenReturn(gitService);

        // Mock tag list (empty) - this is called when gitTag is set
        when(gitService.listTags(any(GroupGitInfoDto.class))).thenReturn(Collections.emptyList());

        // Mock git operations
        doNothing().when(gitService).cloneRepo(any(GroupGitInfoDto.class), anyString());

        // Mock git status with changes - need to mock all methods called by buildStatusDetails
        when(gitStatus.isClean()).thenReturn(false);
        when(gitStatus.getAdded()).thenReturn(Set.of("file1.json"));
        when(gitStatus.getModified()).thenReturn(Collections.emptySet());
        when(gitStatus.getChanged()).thenReturn(Collections.emptySet());
        when(gitStatus.getRemoved()).thenReturn(Collections.emptySet());
        when(gitStatus.getMissing()).thenReturn(Collections.emptySet());
        when(gitStatus.getUntracked()).thenReturn(Collections.emptySet());
        when(gitService.getStatus(anyString())).thenReturn(gitStatus);

        doNothing().when(gitService).commit(any(GroupGitInfoDto.class), anyString(), anyString());
        doNothing().when(gitService).push(any(GroupGitInfoDto.class), anyString());
        doNothing().when(gitService).createTag(any(GroupGitInfoDto.class), anyString(), anyString());

        // Execute
        strategy.exportGroups(request);

        // Verify tag was created
        verify(gitService).createTag(eq(gitInfo), eq("v1.0.0"), anyString());
    }

    @Test
    void testExportGroups_NoChanges() throws Exception {
        // Mock git service router
        when(gitServiceRouter.route(any(GroupGitInfoDto.class))).thenReturn(gitService);

        // Mock git operations
        doNothing().when(gitService).cloneRepo(any(GroupGitInfoDto.class), anyString());

        // Mock git status with no changes - still need to mock all methods for buildStatusDetails
        when(gitStatus.isClean()).thenReturn(true);
        when(gitStatus.getAdded()).thenReturn(Collections.emptySet());
        when(gitStatus.getModified()).thenReturn(Collections.emptySet());
        when(gitStatus.getChanged()).thenReturn(Collections.emptySet());
        when(gitStatus.getRemoved()).thenReturn(Collections.emptySet());
        when(gitStatus.getMissing()).thenReturn(Collections.emptySet());
        when(gitStatus.getUntracked()).thenReturn(Collections.emptySet());
        when(gitService.getStatus(anyString())).thenReturn(gitStatus);

        // Execute
        strategy.exportGroups(request);

        // Verify commit, push, and tag were NOT called
        verify(gitService, never()).commit(any(), anyString(), anyString());
        verify(gitService, never()).push(any(), anyString());
        verify(gitService, never()).createTag(any(), anyString(), anyString());

        // Verify skip step was recorded
        List<GitOperationStep> steps = recordDto.getGitOperationSteps();
        assertTrue(steps.stream().anyMatch(step -> step.getStepName().contains("Skip")));
    }

    @Test
    void testExportGroups_WithoutRecordDto() throws Exception {
        // Remove record DTO
        request.setRecordDto(null);

        // Mock git service router
        when(gitServiceRouter.route(any(GroupGitInfoDto.class))).thenReturn(gitService);

        // Mock git operations
        doNothing().when(gitService).cloneRepo(any(GroupGitInfoDto.class), anyString());

        // Mock git status with changes
        when(gitStatus.isClean()).thenReturn(false);
        when(gitStatus.getAdded()).thenReturn(Set.of("file1.json"));
        when(gitStatus.getModified()).thenReturn(Set.of("file2.json"));
        when(gitStatus.getRemoved()).thenReturn(Set.of("file3.json"));
        when(gitService.getStatus(anyString())).thenReturn(gitStatus);

        doNothing().when(gitService).commit(any(GroupGitInfoDto.class), anyString(), anyString());
        doNothing().when(gitService).push(any(GroupGitInfoDto.class), anyString());

        // Execute - should not throw exception
        assertDoesNotThrow(() -> strategy.exportGroups(request));

        // Verify git operations were still called
        verify(gitService).commit(any(), anyString(), anyString());
        verify(gitService).push(any(), anyString());
    }

    @Test
    void testExportGroups_GitOperationFails() {
        // Mock git service router
        when(gitServiceRouter.route(any(GroupGitInfoDto.class))).thenReturn(gitService);

        // Mock clone to throw exception
        doThrow(new RuntimeException("Clone failed")).when(gitService).cloneRepo(any(GroupGitInfoDto.class), anyString());

        // Execute and expect exception
        BizException exception = assertThrows(BizException.class, () -> strategy.exportGroups(request));
        assertEquals("Git.Export.Failed", exception.getErrorCode());

        // Verify error was recorded in steps
        List<GitOperationStep> steps = recordDto.getGitOperationSteps();
        assertTrue(steps.stream().anyMatch(step ->
            step.getStatus() == GitOperationStep.StepStatus.FAILED
        ));
    }

    @Test
    void testExportGroups_EmptyContents() throws Exception {
        // Set empty contents
        request.setContents(new HashMap<>());

        // Mock git service router
        when(gitServiceRouter.route(any(GroupGitInfoDto.class))).thenReturn(gitService);

        // Mock git operations
        doNothing().when(gitService).cloneRepo(any(GroupGitInfoDto.class), anyString());

        // Mock git status with no changes - need to mock all methods for buildStatusDetails
        when(gitStatus.isClean()).thenReturn(true);
        when(gitStatus.getAdded()).thenReturn(Collections.emptySet());
        when(gitStatus.getModified()).thenReturn(Collections.emptySet());
        when(gitStatus.getChanged()).thenReturn(Collections.emptySet());
        when(gitStatus.getRemoved()).thenReturn(Collections.emptySet());
        when(gitStatus.getMissing()).thenReturn(Collections.emptySet());
        when(gitStatus.getUntracked()).thenReturn(Collections.emptySet());
        when(gitService.getStatus(anyString())).thenReturn(gitStatus);

        // Execute
        strategy.exportGroups(request);

        // Verify no commit was made
        verify(gitService, never()).commit(any(), anyString(), anyString());
    }

    @Test
    void testExportGroups_NullContents() throws Exception {
        // Set null contents
        request.setContents(null);

        // Mock git service router
        when(gitServiceRouter.route(any(GroupGitInfoDto.class))).thenReturn(gitService);

        // Mock git operations
        doNothing().when(gitService).cloneRepo(any(GroupGitInfoDto.class), anyString());

        // Mock git status with no changes - need to mock all methods for buildStatusDetails
        when(gitStatus.isClean()).thenReturn(true);
        when(gitStatus.getAdded()).thenReturn(Collections.emptySet());
        when(gitStatus.getModified()).thenReturn(Collections.emptySet());
        when(gitStatus.getChanged()).thenReturn(Collections.emptySet());
        when(gitStatus.getRemoved()).thenReturn(Collections.emptySet());
        when(gitStatus.getMissing()).thenReturn(Collections.emptySet());
        when(gitStatus.getUntracked()).thenReturn(Collections.emptySet());
        when(gitService.getStatus(anyString())).thenReturn(gitStatus);

        // Execute
        strategy.exportGroups(request);

        // Verify no commit was made
        verify(gitService, never()).commit(any(), anyString(), anyString());
    }
}

