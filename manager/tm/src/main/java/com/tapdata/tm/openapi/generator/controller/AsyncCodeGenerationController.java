package com.tapdata.tm.openapi.generator.controller;

import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.openapi.generator.dto.CodeGenerationRequest;
import com.tapdata.tm.openapi.generator.service.AsyncCodeGenerationService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Async Code Generation Controller
 *
 * @author sam
 * @date 2024/12/19
 */
@Tag(name = "Async Code Generation", description = "Async OpenAPI code generation operations")
@RestController
@RequestMapping("/api/openapi/async")
@Slf4j
public class AsyncCodeGenerationController extends BaseController {

	private final AsyncCodeGenerationService asyncCodeGenerationService;

	public AsyncCodeGenerationController(AsyncCodeGenerationService asyncCodeGenerationService) {
		this.asyncCodeGenerationService = asyncCodeGenerationService;
	}

	/**
	 * Generate code asynchronously
	 *
	 * @param request Code generation request
	 * @return Response message indicating the async task has been started
	 */
	@Operation(summary = "Generate code asynchronously", description = "Start async code generation task and return immediately")
	@PostMapping("/generate")
	public ResponseMessage<String> generateCodeAsync(
			@Parameter(name = "request", description = "Code generation request", required = true)
			@Valid @RequestBody CodeGenerationRequest request) {

		UserDetail userDetail = getLoginUser();
		log.info("Received async code generation request from user: {}, artifactId: {}",
				userDetail.getUserId(), request.getArtifactId());

		try {
			// Start async code generation
			asyncCodeGenerationService.generateCode(request, userDetail);

			String message = String.format("Async code generation task started for artifactId: %s, version: %s",
					request.getArtifactId(), request.getVersion());

			log.info("Async code generation task started successfully for artifactId: {}", request.getArtifactId());

			return success(message);

		} catch (Exception e) {
			if (e instanceof BizException) {
				throw e;
			}
			log.error("Failed to start async code generation for artifactId: {}", request.getArtifactId(), e);
			return failed("openapi.generator.unknown.error", e.getMessage());
		}
	}
}
