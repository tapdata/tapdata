package io.tapdata.flow.engine.manager;

/**
 * @author jackin
 * @date 2021/7/29 4:50 PM
 **/
//public class JetDataFlowFactory {
//
//  public static AbstractProcessor getJetSourceProcessor(
//    Stage stage,
//    Connections connection,
//    SyncTypeEnum syncTypeEnum,
//    DataFlow dataFlow,
//    String userId,
//    List<String> baseURLs,
//    int retryTime,
//    ConfigurationCenter configCenter,
//    DatabaseTypeEnum.DatabaseType databaseType
//  ) {
//    final String version = databaseType.getVersion();
//    final DatabaseTypeEnum.DatabaseTypeVersion connectionVersion = DatabaseTypeEnum.DatabaseTypeVersion.fromString(version);
////    if (DatabaseTypeEnum.DatabaseTypeVersion.VERSION_2_0 == connectionVersion) {
////      final String rootLibDir = getRootLibDir(configCenter);
////      if ("sdc".equals(databaseType.getSource()) && MapUtils.isNotEmpty(connection.getConfig())) {
////        connection.getConfig().put("distDir", rootLibDir);
////      }
////      return null;
////    } else {
//      return new JetDataFlowV1Source(
//        stage,
//        connection,
//        syncTypeEnum,
//        dataFlow,
//        userId,
//        baseURLs,
//        retryTime,
//        configCenter
//      );
////    }
//  }
//
//  public static AbstractProcessor getJetTargetProcessor(
//    Stage stage,
//    Connections connection,
//    DataFlow dataFlow,
//    String userId,
//    List<String> baseURLs,
//    int retryTime,
//    ConfigurationCenter configCenter,
//    DatabaseTypeEnum.DatabaseType databaseType
//  ) {
//    final String version = databaseType.getVersion();
//    final DatabaseTypeEnum.DatabaseTypeVersion connectionVersion = DatabaseTypeEnum.DatabaseTypeVersion.fromString(version);
////    if (DatabaseTypeEnum.DatabaseTypeVersion.VERSION_2_0 == connectionVersion) {
////      final String rootLibDir = getRootLibDir(configCenter);
////      if ("sdc".equals(databaseType.getSource()) && MapUtils.isNotEmpty(connection.getConfig())) {
////        connection.getConfig().put("distDir", rootLibDir);
////      }
////
////      return null;
////    } else {
//
//      return new JetDataFlowV1Target(
//        stage,
//        connection,
//        dataFlow,
//        userId,
//        baseURLs,
//        retryTime,
//        configCenter
//      );
////    }
//  }
//
//  public static String getRootLibDir(ConfigurationCenter configCenter) {
//    String workDir = (String) configCenter.getConfig(ConfigurationCenter.WORK_DIR);
//    if (workDir == null) {
//      workDir = ".";
//    }
//    return workDir + File.separator + "lib";
//  }
//
//}
