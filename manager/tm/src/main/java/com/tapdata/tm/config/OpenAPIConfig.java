package com.tapdata.tm.config;

import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.security.SecurityRequirement;
import io.swagger.v3.oas.models.security.SecurityScheme;
import org.springdoc.core.GroupedOpenApi;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2020/9/12 4:32 下午
 * @description
 */
@Configuration
public class OpenAPIConfig {

	@Bean
	public GroupedOpenApi actuatorApi() {
		return GroupedOpenApi.builder().group("static")
			.pathsToExclude("/")
			//.pathsToExclude("/version")
			.build();
	}

	@Bean
	public OpenAPI customOpenAPI(
								 @Value("${application.title}") String name,
								 @Value("${application.version}") String mainVersion,
								 @Value("${application.commit_version}") String commitVersion,
								 @Value("${application.build}") String build,
								 @Value("${application.description}") String description
	) {
		return new OpenAPI()
			.components(new Components()
					.addSecuritySchemes("basicAuth",
							new SecurityScheme().type(SecurityScheme.Type.HTTP).scheme("basic")))
				.security(Collections.singletonList(new SecurityRequirement() {{
					put("basicAuth", Collections.emptyList());
				}}))
				.info(new Info()
				.title(name + " API Spec")
				.version(String.format("%s (%s - %s) ", mainVersion, commitVersion, build))
				.description(description)
				.contact(new Contact().email("leon@tapdata.net").url("http://www.tapdata.io"))
			);

	}
}
