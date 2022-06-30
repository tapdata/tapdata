package io.tapdata.websocket.handler;

/**
 * @author samuel
 * @Description
 * @create 2021-11-01 09:52
 **/
//@EventHandlerAnnotation(type = "loadVika")
//public class LoadVikaHandler extends BaseEventHandler implements WebSocketEventHandler {
//
//  @Override
//  public Object handle(Map event) {
//    logger.info("Starting load vika resource from websocket event: " + event);
//    if (StringUtils.isBlank((String) event.getOrDefault("load_type", ""))) {
//      return WebSocketEventResult.handleFailed(WebSocketEventResult.Type.LOAD_VIKA_RESULT, "loadType cannot be empty, options: space, node, view, field");
//    }
//    if (StringUtils.isAllBlank((String) event.getOrDefault("api_token", ""), (String) event.getOrDefault("connection_id", ""))) {
//      return WebSocketEventResult.handleFailed(WebSocketEventResult.Type.LOAD_VIKA_RESULT, "api_token, connection_id cannot both be empty");
//    }
//    String vikaBasePath = (String) event.getOrDefault("database_host", "");
//    Vika vika = new VikaV1(getApiToken(event));
//    vika.setVikaBasePath(vikaBasePath);
//    String loadType = (String) event.get("load_type");
//
//    Object result;
//    try {
//      switch (loadType) {
//        case "space":
//          result = vika.getSpaces();
//          break;
//        case "node":
//          if (StringUtils.isBlank((String) event.getOrDefault("space_id", ""))) {
//            return WebSocketEventResult.handleFailed(WebSocketEventResult.Type.LOAD_VIKA_RESULT, "missing space_id when load vika nodes");
//          }
//          if (StringUtils.isNotBlank((String) event.getOrDefault("node_id", ""))) {
//            result = vika.getNodeDetail((String) event.get("space_id"), (String) event.get("node_id"));
//          } else {
//            result = vika.getNodes((String) event.get("space_id"));
//          }
//          break;
//        case "view":
//          if (StringUtils.isBlank((String) event.getOrDefault("node_id", ""))) {
//            return WebSocketEventResult.handleFailed(WebSocketEventResult.Type.LOAD_VIKA_RESULT, "missing node_id when load vika views");
//          }
//          result = vika.getViews((String) event.get("node_id"));
//          break;
//        case "field":
//          if (StringUtils.isAnyBlank((String) event.getOrDefault("space_id", ""), (String) event.getOrDefault("node_id", ""))) {
//            return WebSocketEventResult.handleFailed(WebSocketEventResult.Type.LOAD_VIKA_RESULT, "missing space_id or node_id when load vika fields");
//          }
//          result = vika.getTapdataModel((String) event.get("space_id"), (String) event.get("node_id"));
//          break;
//        default:
//          return WebSocketEventResult.handleFailed(WebSocketEventResult.Type.LOAD_VIKA_RESULT, "unrecognized loadType: " + loadType);
//      }
//    } catch (Exception e) {
//      return WebSocketEventResult.handleFailed(WebSocketEventResult.Type.LOAD_VIKA_RESULT, e.getMessage() + "\n  " + Log4jUtil.getStackString(e), e);
//    }
//    if (result != null) {
//      return WebSocketEventResult.handleSuccess(WebSocketEventResult.Type.LOAD_VIKA_RESULT, result);
//    } else {
//      return WebSocketEventResult.handleFailed(WebSocketEventResult.Type.LOAD_VIKA_RESULT, "load nothing");
//    }
//  }
//
//  private String getApiToken(Map event) {
//    String apiToken;
//    String apiTokenFromEvent = (String) event.getOrDefault("api_token", "");
//    String connectionIdFromEvent = (String) event.getOrDefault("connection_id", "");
//    if (StringUtils.isBlank(apiTokenFromEvent)) {
//      Query query = new Query(Criteria.where("id").is(connectionIdFromEvent));
//      query.fields().exclude("schema");
//      Connections connections = clientMongoOperator.findOne(query, ConnectorConstant.CONNECTION_COLLECTION, Connections.class);
//      connections.decodeDatabasePassword();
//      apiToken = connections.getDatabase_password();
//    } else {
//      apiToken = apiTokenFromEvent;
//    }
//    return apiToken;
//  }
//
//  public static void main(String[] args) {
//    Map map = new HashMap();
//    LoadVikaHandler loadVikaHandler = new LoadVikaHandler();
//    Object result;
//
//    // load spaces
//    System.out.println("\n=== load spaces ===");
//    map.put("load_type", "space");
//    map.put("api_token", "usk2GwBFmPzYis4DOU2IDG0");
//    result = loadVikaHandler.handle(map);
//    System.out.println(result);
//
//    // load nodes
//    System.out.println("\n=== load nodes ===");
//    map.clear();
//    map.put("load_type", "node");
//    map.put("api_token", "usk2GwBFmPzYis4DOU2IDG0");
//    map.put("space_id", "spctjCKGJ5cFf");
//    result = loadVikaHandler.handle(map);
//    System.out.println(result);
//
//    System.out.println("\n=== load node detail ===");
//    map.clear();
//    map.put("load_type", "node");
//    map.put("api_token", "usk2GwBFmPzYis4DOU2IDG0");
//    map.put("space_id", "spctjCKGJ5cFf");
//    map.put("node_id", "dstBnipW73vly0Ztpk");
//    result = loadVikaHandler.handle(map);
//    System.out.println(result);
//
//    // load views
//    System.out.println("\n=== load views ===");
//    map.clear();
//    map.put("load_type", "view");
//    map.put("api_token", "usk2GwBFmPzYis4DOU2IDG0");
//    map.put("node_id", "dstBnipW73vly0Ztpk");
//    result = loadVikaHandler.handle(map);
//    System.out.println(result);
//
//    // load fields
//    System.out.println("\n=== load fields ===");
//    map.clear();
//    map.put("load_type", "field");
//    map.put("api_token", "usk2GwBFmPzYis4DOU2IDG0");
//    map.put("node_id", "dstBnipW73vly0Ztpk");
//    result = loadVikaHandler.handle(map);
//    System.out.println(result);
//  }
//}
