package com.tapdata.tm.openapi.generator.util;

import com.tapdata.tm.openapi.generator.dto.CodeGenerationRequest;
import com.tapdata.tm.openapi.generator.exception.CodeGenerationException;
import com.tapdata.tm.commons.util.JsonUtil;
import io.swagger.v3.core.util.Json;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.PathItem;
import io.swagger.v3.oas.models.Paths;
import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.parameters.Parameter;
import io.swagger.v3.oas.models.security.OAuthFlows;
import io.swagger.v3.oas.models.security.SecurityScheme;
import io.swagger.v3.oas.models.media.Schema;
import io.swagger.v3.oas.models.media.MediaType;
import io.swagger.v3.oas.models.media.Content;
import io.swagger.v3.oas.models.parameters.RequestBody;
import io.swagger.v3.parser.OpenAPIV3Parser;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

/**
 * OpenAPI JSON processing utility class
 * Handles OpenAPI JSON content transformation using Swagger Models
 *
 * @author sam
 * @date 2024/12/19
 */
@Slf4j
public class OpenApiJsonProcessor {

    private final RestTemplate restTemplate;

    public OpenApiJsonProcessor(RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
    }

    /**
     * Process OpenAPI JSON from URL and return temporary file path
     *
     * @param request CodeGenerationRequest containing the OAS URL
     * @param tempDir Temporary directory for file creation
     * @return Path to the temporary file containing processed OpenAPI JSON
     * @throws CodeGenerationException if processing fails
     */
    public Path processOpenapiJson(CodeGenerationRequest request, Path tempDir) throws CodeGenerationException {
        String oasUrl = request.getOas();
        log.info("Starting to process OpenAPI JSON from URL: {}", oasUrl);

        try {
            // Step 1: Download OpenAPI JSON content from URL
            String jsonContent = downloadOpenapiJson(oasUrl);

            // Step 2: Parse JSON into OpenAPI model
            OpenAPI openAPI = parseJsonToOpenAPI(jsonContent, oasUrl);

            // Step 3: Process the OpenAPI model (custom processing logic)
            OpenAPI processedOpenAPI = processOpenAPI(openAPI, request);

            // Step 4: Write processed OpenAPI to temporary file
            Path tempFile = writeOpenAPIToTempFile(processedOpenAPI, tempDir);

            log.info("Successfully processed OpenAPI JSON and created temporary file: {}", tempFile);
            return tempFile;

        } catch (Exception e) {
            if (e instanceof CodeGenerationException) {
                throw e;
            }
            log.error("Unexpected error while processing OpenAPI JSON from URL: {}", oasUrl, e);
            throw new CodeGenerationException("Failed to download or process OpenAPI JSON from URL: " + oasUrl + ". Error: " + e.getMessage(), e);
        }
    }

    /**
     * Download OpenAPI JSON content from the specified URL
     *
     * @param oasUrl The URL to download OpenAPI JSON from
     * @return The JSON content as string
     * @throws CodeGenerationException if download fails
     */
    private String downloadOpenapiJson(String oasUrl) throws CodeGenerationException {
        log.debug("Making HTTP GET request to: {}", oasUrl);

        try {
            String jsonResponse = this.restTemplate.getForObject(oasUrl, String.class);

            if (jsonResponse == null || jsonResponse.trim().isEmpty()) {
                throw new CodeGenerationException("Received empty response from OpenAPI URL: " + oasUrl);
            }

            log.info("Successfully downloaded OpenAPI JSON, content length: {} characters", jsonResponse.length());
            return jsonResponse;

        } catch (Exception e) {
            log.error("Failed to download OpenAPI JSON from URL: {}", oasUrl, e);
            throw new CodeGenerationException("Failed to download OpenAPI JSON from URL: " + oasUrl + ". Error: " + e.getMessage(), e);
        }
    }

    /**
     * Parse JSON string into OpenAPI model object
     *
     * @param jsonContent The JSON content to parse
     * @param oasUrl      The original URL (for error reporting)
     * @return Parsed OpenAPI object
     * @throws CodeGenerationException if parsing fails
     */
    private OpenAPI parseJsonToOpenAPI(String jsonContent, String oasUrl) throws CodeGenerationException {
        try {
            // Use OpenAPIV3Parser to parse JSON content
            OpenAPIV3Parser parser = new OpenAPIV3Parser();
            OpenAPI openAPI = parser.readContents(jsonContent).getOpenAPI();

            if (openAPI == null) {
                throw new CodeGenerationException("Failed to parse OpenAPI JSON or received invalid content from: " + oasUrl);
            }

            log.info("Successfully parsed OpenAPI JSON into OpenAPI model");
            log.debug("OpenAPI info: title={}, version={}", 
                openAPI.getInfo() != null ? openAPI.getInfo().getTitle() : "N/A",
                openAPI.getInfo() != null ? openAPI.getInfo().getVersion() : "N/A");

            return openAPI;

        } catch (Exception e) {
            if (e instanceof CodeGenerationException) {
                throw e;
            }
            log.error("Failed to parse JSON response from URL: {}", oasUrl, e);
            throw new CodeGenerationException("Invalid JSON format received from OpenAPI URL: " + oasUrl + ". Error: " + e.getMessage(), e);
        }
    }

    /**
     * Process the OpenAPI model with custom logic
     *
     * @param openAPI The OpenAPI model to process
     * @param request CodeGenerationRequest containing processing parameters
     * @return A new processed OpenAPI model
     */
    private OpenAPI processOpenAPI(OpenAPI openAPI, CodeGenerationRequest request) {
        log.debug("Processing OpenAPI model with custom logic");

        // Create a new OpenAPI object as a copy
        OpenAPI processedOpenAPI = new OpenAPI();

        // Copy basic information
        processedOpenAPI.setOpenapi(openAPI.getOpenapi());
        processedOpenAPI.setInfo(openAPI.getInfo());
        processedOpenAPI.setServers(openAPI.getServers());
        processedOpenAPI.setTags(openAPI.getTags());
        processedOpenAPI.setExternalDocs(openAPI.getExternalDocs());

        // Initialize components if not exists
        Components processedComponents = openAPI.getComponents() != null ?
            processComponents(openAPI.getComponents()) : new Components();

        // Process paths and extract filter schemas
        if (openAPI.getPaths() != null) {
            Paths processedPaths = processPaths(openAPI.getPaths(), request);

            // Extract filter schemas from processed GET operations and add to components
            extractAndAddFilterSchemas(processedPaths, processedComponents, request);

            // Process response schemas to modify count fields
            processResponseSchemas(processedPaths, processedComponents);

            processedOpenAPI.setPaths(processedPaths);
        }

        // Set processed components
        processedOpenAPI.setComponents(processedComponents);

        // Copy other fields
        processedOpenAPI.setSecurity(openAPI.getSecurity());
        processedOpenAPI.setExtensions(openAPI.getExtensions());

        log.debug("Processed OpenAPI model successfully");
        return processedOpenAPI;
    }

    /**
     * Extract filter schemas from GET operations and add them to components/schemas
     *
     * @param paths      Original paths
     * @param components Components to add schemas to
     * @param request    CodeGenerationRequest containing module IDs for filtering
     */
    private void extractAndAddFilterSchemas(Paths paths, Components components, CodeGenerationRequest request) {
        if (paths == null || paths.isEmpty()) {
            return;
        }

        log.debug("Starting to extract filter schemas from GET operations");
        int extractedCount = 0;

        for (Map.Entry<String, PathItem> pathEntry : paths.entrySet()) {
            String pathKey = pathEntry.getKey();
            PathItem pathItem = pathEntry.getValue();

            // Only process GET operations
            Operation getOperation = pathItem.getGet();
            if (getOperation == null) {
                continue;
            }

            // Check if operation has x-api-id extension and is in requested modules
            Object apiIdExtension = getOperation.getExtensions() != null ?
                getOperation.getExtensions().get("x-api-id") : null;
            if (!(apiIdExtension instanceof String)) {
                continue;
            }

            String apiId = (String) apiIdExtension;
            if (!request.getModuleIds().contains(apiId)) {
                continue;
            }

            // Look for filter parameter
            if (getOperation.getParameters() != null) {
                for (Parameter parameter : getOperation.getParameters()) {
                    if ("filter".equals(parameter.getName()) && parameter.getSchema() != null) {
                        // Get x-table-name from path extensions
                        String tableName = extractTableNameFromPath(pathItem, getOperation);
                        if (tableName != null) {
                            String schemaKey = tableName + "_filter";

                            // Add schema to components
                            if (components.getSchemas() == null) {
                                components.setSchemas(new HashMap<>());
                            }

                            // Clone the schema to avoid modifying the original
                            Schema<?> filterSchema = cloneSchema(parameter.getSchema());

                            // Transform the filter schema before adding to components
                            transformFilterSchema(filterSchema);

                            components.getSchemas().put(schemaKey, filterSchema);

                            // Replace parameter schema with $ref
                            Schema<?> refSchema = new Schema<>();
                            refSchema.set$ref("#/components/schemas/" + schemaKey);
                            parameter.setSchema(refSchema);

                            extractedCount++;
                            log.debug("Extracted filter schema for table '{}' as '{}'", tableName, schemaKey);

                            // Find POST operation with same x-api-id and add requestBody
                            addRequestBodyToPostOperation(paths, apiId, schemaKey);
                        }
                    }
                }
            }
        }

        log.debug("Extracted {} filter schemas to components/schemas", extractedCount);
    }

    /**
     * Find POST operation with same x-api-id and add requestBody with filter schema reference
     *
     * @param paths     All paths to search through
     * @param apiId     The x-api-id to match
     * @param schemaKey The schema key to reference in requestBody
     */
    private void addRequestBodyToPostOperation(Paths paths, String apiId, String schemaKey) {
        if (paths == null || paths.isEmpty()) {
            return;
        }

        log.debug("Looking for POST operation with x-api-id '{}' to add requestBody", apiId);

        for (Map.Entry<String, PathItem> pathEntry : paths.entrySet()) {
            String pathKey = pathEntry.getKey();
            PathItem pathItem = pathEntry.getValue();

            // Only process POST operations
            Operation postOperation = pathItem.getPost();
            if (postOperation == null) {
                continue;
            }

            // Check if operation has matching x-api-id extension
            Object postApiIdExtension = postOperation.getExtensions() != null ?
                postOperation.getExtensions().get("x-api-id") : null;
            if (!(postApiIdExtension instanceof String)) {
                continue;
            }

            String postApiId = (String) postApiIdExtension;
            if (!apiId.equals(postApiId)) {
                continue;
            }

            // Create requestBody with filter schema reference
            RequestBody requestBody = new RequestBody();
            requestBody.setRequired(false);

            // Create content with application/json media type
            Content content = new Content();
            MediaType mediaType = new MediaType();

            // Create schema with $ref to the filter schema
            Schema<?> refSchema = new Schema<>();
            refSchema.set$ref("#/components/schemas/" + schemaKey);
            mediaType.setSchema(refSchema);

            content.addMediaType("application/json", mediaType);
            requestBody.setContent(content);

            // Set requestBody to POST operation
            postOperation.setRequestBody(requestBody);

            log.debug("Added requestBody with schema reference '{}' to POST operation at path '{}'", schemaKey, pathKey);
            return; // Found and processed, exit the loop
        }

        log.debug("No POST operation found with x-api-id '{}'", apiId);
    }

    /**
     * Transform filter schema by removing offset, changing skip to page, and setting default values
     *
     * @param filterSchema The filter schema to transform
     */
    private void transformFilterSchema(Schema<?> filterSchema) {
        if (filterSchema == null || filterSchema.getProperties() == null) {
            return;
        }

        Map<String, Schema> properties = filterSchema.getProperties();

        // Remove offset property if it exists
        if (properties.containsKey("offset")) {
            properties.remove("offset");
            log.debug("Removed 'offset' property from filter schema");
        }

        // Change skip to page if skip exists
        if (properties.containsKey("skip")) {
            Schema skipSchema = properties.remove("skip");
            properties.put("page", skipSchema);
            log.debug("Changed 'skip' property to 'page' in filter schema");
        }

        // Set default values for limit and page properties
        if (properties.containsKey("limit")) {
            Schema limitSchema = properties.get("limit");
            if (limitSchema != null) {
                limitSchema.setDefault(10);
                log.debug("Set default value 10 for limit property in filter schema");
            }
        }

        if (properties.containsKey("page")) {
            Schema pageSchema = properties.get("page");
            if (pageSchema != null) {
                pageSchema.setDefault(1);
                log.debug("Set default value 1 for page property in filter schema");
            }
        }
    }

    /**
     * Process response schemas to modify count fields
     *
     * @param paths      Processed paths containing operations
     * @param components Components containing schemas
     */
    private void processResponseSchemas(Paths paths, Components components) {
        if (paths == null || paths.isEmpty()) {
            return;
        }

        log.debug("Starting to process response schemas to modify count fields");
        int modifiedCount = 0;

        for (Map.Entry<String, PathItem> pathEntry : paths.entrySet()) {
            String pathKey = pathEntry.getKey();
            PathItem pathItem = pathEntry.getValue();

            // Process all operations in this path
            modifiedCount += processOperationResponses(pathItem.getGet(), pathKey, "GET", components);
            modifiedCount += processOperationResponses(pathItem.getPost(), pathKey, "POST", components);
            modifiedCount += processOperationResponses(pathItem.getPut(), pathKey, "PUT", components);
            modifiedCount += processOperationResponses(pathItem.getDelete(), pathKey, "DELETE", components);
            modifiedCount += processOperationResponses(pathItem.getOptions(), pathKey, "OPTIONS", components);
            modifiedCount += processOperationResponses(pathItem.getHead(), pathKey, "HEAD", components);
            modifiedCount += processOperationResponses(pathItem.getPatch(), pathKey, "PATCH", components);
            modifiedCount += processOperationResponses(pathItem.getTrace(), pathKey, "TRACE", components);
        }

        log.debug("Modified {} count fields in response schemas", modifiedCount);
    }

    /**
     * Process responses for a single operation
     *
     * @param operation  The operation to process (can be null)
     * @param pathKey    The path key for logging
     * @param method     The HTTP method for logging
     * @param components Components containing schemas
     * @return Number of count fields modified
     */
    private int processOperationResponses(Operation operation, String pathKey, String method, Components components) {
        if (operation == null || operation.getResponses() == null) {
            return 0;
        }

        int modifiedCount = 0;
        log.debug("Processing responses for {} {}", method, pathKey);

        for (Map.Entry<String, io.swagger.v3.oas.models.responses.ApiResponse> responseEntry : operation.getResponses().entrySet()) {
            String responseCode = responseEntry.getKey();
            io.swagger.v3.oas.models.responses.ApiResponse apiResponse = responseEntry.getValue();

            if (apiResponse.getContent() != null) {
                for (Map.Entry<String, MediaType> contentEntry : apiResponse.getContent().entrySet()) {
                    String mediaType = contentEntry.getKey();
                    MediaType mediaTypeObj = contentEntry.getValue();

                    if (mediaTypeObj.getSchema() != null) {
                        modifiedCount += processResponseSchema(mediaTypeObj.getSchema(), components, pathKey, method, responseCode, mediaType);
                    }
                }
            }
        }

        return modifiedCount;
    }

    /**
     * Process a response schema to modify count fields
     *
     * @param schema       The schema to process
     * @param components   Components containing referenced schemas
     * @param pathKey      The path key for logging
     * @param method       The HTTP method for logging
     * @param responseCode The response code for logging
     * @param mediaType    The media type for logging
     * @return Number of count fields modified
     */
    private int processResponseSchema(Schema<?> schema, Components components, String pathKey, String method, String responseCode, String mediaType) {
        if (schema == null) {
            return 0;
        }

        int modifiedCount = 0;

        // Handle $ref schemas - resolve the reference and process the referenced schema
        if (schema.get$ref() != null) {
            String ref = schema.get$ref();
            if (ref.startsWith("#/components/schemas/")) {
                String schemaName = ref.substring("#/components/schemas/".length());
                if (components.getSchemas() != null && components.getSchemas().containsKey(schemaName)) {
                    Schema<?> referencedSchema = components.getSchemas().get(schemaName);
                    modifiedCount += processSchemaProperties(referencedSchema, pathKey, method, responseCode, mediaType);
                }
            }
        } else {
            // Handle direct schemas
            modifiedCount += processSchemaProperties(schema, pathKey, method, responseCode, mediaType);
        }

        return modifiedCount;
    }

    /**
     * Process schema properties recursively to find and modify count fields
     *
     * @param schema       The schema to process
     * @param pathKey      The path key for logging
     * @param method       The HTTP method for logging
     * @param responseCode The response code for logging
     * @param mediaType    The media type for logging
     * @return Number of count fields modified
     */
    private int processSchemaProperties(Schema<?> schema, String pathKey, String method, String responseCode, String mediaType) {
        if (schema == null) {
            return 0;
        }

        int modifiedCount = 0;

        // Process properties if they exist
        if (schema.getProperties() != null) {
            Map<String, Schema> properties = schema.getProperties();

            // Look for count field and modify it
            if (properties.containsKey("count")) {
                Schema countSchema = properties.get("count");
                if (countSchema != null) {
                    countSchema.setType("integer");
                    countSchema.setFormat("int32");
                    modifiedCount++;
                    log.debug("Modified 'count' field in response schema for {} {} {} {} {}",
                        method, pathKey, responseCode, mediaType, "-> {type: 'integer', format: 'int32'}");
                }
            }

            // Recursively process nested object properties
            for (Map.Entry<String, Schema> propertyEntry : properties.entrySet()) {
                Schema propertySchema = propertyEntry.getValue();
                if (propertySchema != null && "object".equals(propertySchema.getType())) {
                    modifiedCount += processSchemaProperties(propertySchema, pathKey, method, responseCode, mediaType);
                }
            }
        }

        // Process array items if this is an array schema
        if (schema.getItems() != null) {
            modifiedCount += processSchemaProperties(schema.getItems(), pathKey, method, responseCode, mediaType);
        }

        // Process additional properties if they exist and are schema objects
        if (schema.getAdditionalProperties() instanceof Schema) {
            Schema additionalPropsSchema = (Schema) schema.getAdditionalProperties();
            modifiedCount += processSchemaProperties(additionalPropsSchema, pathKey, method, responseCode, mediaType);
        }

        return modifiedCount;
    }

    /**
     * Process paths with custom filtering and parameter modifications
     *
     * @param paths   Original paths
     * @param request CodeGenerationRequest containing module IDs for filtering
     * @return Processed paths
     */
    private Paths processPaths(Paths paths, CodeGenerationRequest request) {
        Paths processedPaths = new Paths();
        
        if (paths == null || paths.isEmpty()) {
            return processedPaths;
        }

        for (Map.Entry<String, PathItem> pathEntry : paths.entrySet()) {
            String pathKey = pathEntry.getKey();
            PathItem pathItem = pathEntry.getValue();
            
            PathItem processedPathItem = processPathItem(pathItem, request);
            
            // Only add path if it has operations after processing
            if (hasOperations(processedPathItem)) {
                processedPaths.addPathItem(pathKey, processedPathItem);
            }
        }

        log.debug("Processed {} paths, kept {} paths after filtering", 
            paths.size(), processedPaths.size());
        
        return processedPaths;
    }

    /**
     * Process individual path item and its operations
     *
     * @param pathItem Original path item
     * @param request  CodeGenerationRequest containing module IDs for filtering
     * @return Processed path item
     */
    private PathItem processPathItem(PathItem pathItem, CodeGenerationRequest request) {
        PathItem processedPathItem = new PathItem();
        
        // Copy basic properties
        processedPathItem.setSummary(pathItem.getSummary());
        processedPathItem.setDescription(pathItem.getDescription());
        processedPathItem.setServers(pathItem.getServers());
        processedPathItem.setParameters(pathItem.getParameters());
        
        // Process each HTTP method operation
        processOperation(pathItem.getGet(), processedPathItem::setGet, request);

        // Special filtering for POST operations: skip if x-operation-name starts with "customerQuery"
        if (pathItem.getPost() != null && shouldSkipPostOperation(pathItem.getPost())) {
            log.debug("Skipping POST operation with x-operation-name starting with 'customerQuery'");
        } else {
            processOperation(pathItem.getPost(), processedPathItem::setPost, request);
        }

        processOperation(pathItem.getPut(), processedPathItem::setPut, request);
        processOperation(pathItem.getDelete(), processedPathItem::setDelete, request);
        processOperation(pathItem.getOptions(), processedPathItem::setOptions, request);
        processOperation(pathItem.getHead(), processedPathItem::setHead, request);
        processOperation(pathItem.getPatch(), processedPathItem::setPatch, request);
        processOperation(pathItem.getTrace(), processedPathItem::setTrace, request);
        
        return processedPathItem;
    }

    /**
     * Check if a POST operation should be skipped based on x-operation-name extension
     *
     * @param postOperation The POST operation to check
     * @return true if the operation should be skipped, false otherwise
     */
    private boolean shouldSkipPostOperation(Operation postOperation) {
        if (postOperation == null || postOperation.getExtensions() == null) {
            return false;
        }

        Object operationNameExtension = postOperation.getExtensions().get("x-operation-name");
        if (!(operationNameExtension instanceof String)) {
            return false;
        }

        String operationName = (String) operationNameExtension;
        boolean shouldSkip = StringUtils.startsWith(operationName, "customerQuery");

        if (shouldSkip) {
            log.debug("POST operation with x-operation-name '{}' will be skipped", operationName);
        }

        return shouldSkip;
    }

    /**
     * Process individual operation with filtering and parameter modifications
     *
     * @param operation Original operation
     * @param setter    Method to set the processed operation
     * @param request   CodeGenerationRequest containing module IDs for filtering
     */
    private void processOperation(Operation operation, java.util.function.Consumer<Operation> setter, CodeGenerationRequest request) {
        if (operation == null) {
            return;
        }

        // Check if operation has x-api-id extension
        Object apiIdExtension = operation.getExtensions() != null ? operation.getExtensions().get("x-api-id") : null;
        if (!(apiIdExtension instanceof String)) {
            return; // Skip operations without x-api-id
        }

        String apiId = (String) apiIdExtension;
        if (!request.getModuleIds().contains(apiId)) {
            return; // Skip operations not in requested modules
        }

        // Process parameters for GET operations
        if (operation.getParameters() != null) {
            List<Parameter> processedParameters = operation.getParameters().stream()
                .filter(param -> !"filename".equals(param.getName()))
                .peek(param -> {
                    // Modify page and limit parameters to be integers and set default values
                    if (StringUtils.equalsAny(param.getName(), "page", "limit")) {
                        if (param.getSchema() != null) {
                            param.getSchema().setType("integer");
                            param.getSchema().setFormat("int32");

                            // Set default values: limit=10, page=1
                            if ("limit".equals(param.getName())) {
                                param.getSchema().setDefault(10);
                                log.debug("Set default value 10 for limit parameter in operation");
                            } else if ("page".equals(param.getName())) {
                                param.getSchema().setDefault(1);
                                log.debug("Set default value 1 for page parameter in operation");
                            }
                        }
                    }
                })
                .collect(Collectors.toList());

            operation.setParameters(processedParameters);
        }

        setter.accept(operation);
    }

    /**
     * Check if path item has any operations
     *
     * @param pathItem Path item to check
     * @return true if has operations, false otherwise
     */
    private boolean hasOperations(PathItem pathItem) {
        return pathItem.getGet() != null || pathItem.getPost() != null ||
               pathItem.getPut() != null || pathItem.getDelete() != null ||
               pathItem.getOptions() != null || pathItem.getHead() != null ||
               pathItem.getPatch() != null || pathItem.getTrace() != null;
    }

    /**
     * Process components with security scheme modifications
     *
     * @param components Original components
     * @return Processed components
     */
    private Components processComponents(Components components) {
        if (components == null) {
            return null;
        }

        // Create a new Components object as a copy
        Components processedComponents = new Components();

        // Copy all fields
        processedComponents.setSchemas(components.getSchemas());
        processedComponents.setResponses(components.getResponses());
        processedComponents.setParameters(components.getParameters());
        processedComponents.setExamples(components.getExamples());
        processedComponents.setRequestBodies(components.getRequestBodies());
        processedComponents.setHeaders(components.getHeaders());
        processedComponents.setLinks(components.getLinks());
        processedComponents.setCallbacks(components.getCallbacks());
        processedComponents.setExtensions(components.getExtensions());

        // Process security schemes
        if (components.getSecuritySchemes() != null) {
            Map<String, SecurityScheme> processedSecuritySchemes = processSecuritySchemes(components.getSecuritySchemes());
            processedComponents.setSecuritySchemes(processedSecuritySchemes);
        } else {
            processedComponents.setSecuritySchemes(components.getSecuritySchemes());
        }

        log.debug("Processed components successfully");
        return processedComponents;
    }

    /**
     * Process security schemes, specifically removing implicit OAuth2 flow
     *
     * @param securitySchemes Original security schemes
     * @return Processed security schemes
     */
    private Map<String, SecurityScheme> processSecuritySchemes(Map<String, SecurityScheme> securitySchemes) {
        Map<String, SecurityScheme> processedSchemes = new HashMap<>();

        for (Map.Entry<String, SecurityScheme> entry : securitySchemes.entrySet()) {
            String schemeName = entry.getKey();
            SecurityScheme scheme = entry.getValue();

            if ("OAuth2".equals(schemeName) && scheme.getFlows() != null) {
                // Create a new SecurityScheme to avoid modifying the original
                SecurityScheme processedScheme = new SecurityScheme();
                processedScheme.setType(scheme.getType());
                processedScheme.setDescription(scheme.getDescription());
                processedScheme.setName(scheme.getName());
                processedScheme.setIn(scheme.getIn());
                processedScheme.setScheme(scheme.getScheme());
                processedScheme.setBearerFormat(scheme.getBearerFormat());
                processedScheme.setOpenIdConnectUrl(scheme.getOpenIdConnectUrl());
                processedScheme.setExtensions(scheme.getExtensions());

                // Process OAuth flows - remove implicit flow
                OAuthFlows originalFlows = scheme.getFlows();
                OAuthFlows processedFlows = new OAuthFlows();

                // Copy all flows except implicit
                processedFlows.setAuthorizationCode(originalFlows.getAuthorizationCode());
                processedFlows.setClientCredentials(originalFlows.getClientCredentials());
                processedFlows.setPassword(originalFlows.getPassword());
                // Explicitly skip implicit flow: processedFlows.setImplicit(null);
                processedFlows.setExtensions(originalFlows.getExtensions());

                processedScheme.setFlows(processedFlows);
                processedSchemes.put(schemeName, processedScheme);

                log.debug("Removed implicit OAuth2 flow from security scheme: {}", schemeName);
            } else {
                // For non-OAuth2 schemes or OAuth2 without flows, copy as-is
                processedSchemes.put(schemeName, scheme);
            }
        }

        return processedSchemes;
    }

    /**
     * Extract table name from path extensions
     *
     * @param pathItem  Path item to check
     * @param operation Operation to check
     * @return Table name or null if not found
     */
    private String extractTableNameFromPath(PathItem pathItem, Operation operation) {
        // First try to get from operation extensions
        if (operation.getExtensions() != null) {
            Object tableName = operation.getExtensions().get("x-table-name");
            if (tableName instanceof String) {
                return (String) tableName;
            }
        }

        // Then try to get from path item extensions
        if (pathItem.getExtensions() != null) {
            Object tableName = pathItem.getExtensions().get("x-table-name");
            if (tableName instanceof String) {
                return (String) tableName;
            }
        }

        return null;
    }

    /**
     * Clone a schema object to avoid modifying the original
     *
     * @param originalSchema Original schema to clone
     * @return Cloned schema
     */
    private Schema<?> cloneSchema(Schema<?> originalSchema) {
        if (originalSchema == null) {
            return null;
        }

        try {
            // Use JSON serialization/deserialization for deep cloning
            String jsonString = Json.mapper().writeValueAsString(originalSchema);
            return Json.mapper().readValue(jsonString, Schema.class);
        } catch (Exception e) {
            log.warn("Failed to clone schema using JSON serialization, falling back to manual copy", e);

            // Fallback to manual copying of basic properties
            Schema<?> clonedSchema = new Schema<>();
            clonedSchema.setType(originalSchema.getType());
            clonedSchema.setFormat(originalSchema.getFormat());
            clonedSchema.setTitle(originalSchema.getTitle());
            clonedSchema.setDescription(originalSchema.getDescription());
            clonedSchema.setDefault(originalSchema.getDefault());
            clonedSchema.setExample(originalSchema.getExample());
            clonedSchema.setRequired(originalSchema.getRequired());
            clonedSchema.setProperties(originalSchema.getProperties());
            clonedSchema.setAdditionalProperties(originalSchema.getAdditionalProperties());
            clonedSchema.setItems(originalSchema.getItems());
            // Use raw type to avoid generic type mismatch
            ((Schema) clonedSchema).setEnum(originalSchema.getEnum());
            clonedSchema.setExtensions(originalSchema.getExtensions());

            return clonedSchema;
        }
    }

    /**
     * Write the processed OpenAPI model to a temporary file
     *
     * @param openAPI The OpenAPI model to write
     * @param tempDir Temporary directory for file creation
     * @return Path to the created temporary file
     * @throws CodeGenerationException if file operations fail
     */
    private Path writeOpenAPIToTempFile(OpenAPI openAPI, Path tempDir) throws CodeGenerationException {
        try {
            // Serialize OpenAPI model to JSON using Swagger's Json utility
            String serializedJson = Json.pretty(openAPI);
            log.debug("Successfully serialized OpenAPI model to JSON, length: {} characters", serializedJson.length());

            // Create temporary file
            String tempFileName = "openapi-" + UUID.randomUUID() + ".json";
            Path tempFile = tempDir.resolve(tempFileName);

            // Write JSON content to temporary file
            Files.writeString(tempFile, serializedJson);
            log.info("Successfully wrote OpenAPI JSON to temporary file: {}", tempFile);
            log.debug("Temporary file size: {} bytes", Files.size(tempFile));

            return tempFile;

        } catch (Exception e) {
            log.error("Failed to write OpenAPI model to temporary file", e);
            throw new CodeGenerationException("Failed to write OpenAPI data to temporary file: " + e.getMessage(), e);
        }
    }

    /**
     * Create secure temporary directory for JSON files
     *
     * @param baseTempDir Base temporary directory
     * @return Path to created secure directory
     * @throws IOException if directory creation fails
     */
    public static Path createSecureTempDirectoryForJson(Path baseTempDir) throws IOException {
        // Create a unique subdirectory for this operation
        String dirName = "openapi-json-" + UUID.randomUUID();
        Path tempDir = baseTempDir.resolve(dirName);

        // Create directory with restricted permissions
        Files.createDirectories(tempDir);

        log.debug("Created secure temporary directory for JSON files: {}", tempDir);
        return tempDir;
    }
}
