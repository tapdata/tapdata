package io.tapdata.constant;

public enum TapLog {

  /**
   * INFO
   */
  // Connector
  CON_LOG_0001("Starting connector, initializing."),
  CON_LOG_0002("Starting initial sync."),
  CON_LOG_0003("All initial sync data were received from source."),
  CON_LOG_0004("Start analyzing redo logs."),
  CON_LOG_0005("Start reading after data from {}, redo logs size: {}, read times: {}."),
  CON_LOG_0006("Finished read after data from oracle, spent: {}ms."),
  CON_LOG_0007("Job analysis redo logs spent: {} ms, process spent: {}ms, logs: {}."),
  CON_LOG_0008("Transaction {} size reach {}."),
  CON_LOG_0009("Custom sql incremental mode on, interval: {} ms, valid table(s): [{}]."),
  CON_LOG_0010("Custom sql incremental mode off."),
  CON_LOG_0011("Starting create test data, test write: {}, rows: {}, col: {}."),
  CON_LOG_0012("Thread ready: {}."),
  CON_LOG_0013("Finished stop connector, job name: {}."),
  CON_LOG_0014("Stopped {}'s connector."),
  CON_LOG_0015("Stopping connector runners,please wait."),
  CON_LOG_0016("Forcing stop connector runners,please wait."),
  CON_LOG_0017("Stop connector time out(waiting mills: {}), force stopping connector."),
  CON_LOG_0018("Read after data time: {}."),
  CON_LOG_0019("Starting clear trigger log."),
  CON_LOG_0020("Start reading cdc record table, query table per batch interval: {} ms, buffer limit: {}, offset ahead: {} milliseconds."),
  CON_LOG_0021("Starting validate connections name: {}."),
  CON_LOG_0022("Finished validate connections : {}, result: {}, engine spend: {} ms, management spend: {} ms, schema count: {}."),
  CON_LOG_0023("Stopping connector runners, all rest api call failed, please wait."),
  CON_LOG_0024("Starting cdc sync."),
  CON_LOG_0025("Found {} files(s) to sync."),
  CON_LOG_0026("Finished to read file(s) meta data, put file(s) meta data in queue."),
  CON_LOG_0027("Finished to analysis {} file: {}, spend time: {} seconds, total rows: {}."),
  CON_LOG_0028("Succeed to add online redo log file in logmnr: {}"),
  CON_LOG_0029("Succeed to start logmnr."),
  CON_LOG_0030("Initializing javascript engine."),
  CON_LOG_0031("Finished add custom build in function[rest, core, log, sleep] into javascript engine."),
  CON_LOG_0032("Start running initial sync script, waiting for data..."),
  CON_LOG_0033("Start running increamental sync script, interval: {} millsecond(s)."),
  CON_LOG_0034("Initial sync is finished, spend time: {} seconds."),
  CON_LOG_0035("Initial sync is finished, script is empty."),
  CON_LOG_0036("Current offset {}"),

  // Transformer
  TRAN_LOG_0001("Created index {} for target collection {}."),
  TRAN_LOG_0002("Waiting for child threads to complete."),
  TRAN_LOG_0003("Completed collect stats data."),
  TRAN_LOG_0004("Completed initial sync."),
  TRAN_LOG_0006("Start CDC processing."),
  TRAN_LOG_0007("Dropping target collections."),
  TRAN_LOG_0008("Dropped collection: [[ {} ]]."),
  TRAN_LOG_0009("Transformation blocked. Transform queue size: {}."),
  TRAN_LOG_0010("Target database write queue blocked. Current thread {}, process msg spent {} ms, queue size {}, other threads [{}]."),
  TRAN_LOG_0011("Dropping target collections which source tables without primary key."),
  TRAN_LOG_0012("Starting validator listen node {}'s oplog, host: {}."),
  TRAN_LOG_0013("Starting verify data consistency, sample rate: {}%."),
  TRAN_LOG_0014("No collections found need to be dropped."),
  TRAN_LOG_0015("Switching table [{}] to [{}]."),
  TRAN_LOG_0016("Finished switch table."),
  TRAN_LOG_0017("Turned upsert on."),
  TRAN_LOG_0018("Turned upsert off."),
  TRAN_LOG_0019("Forcing stop transformer runners,please wait."),
  TRAN_LOG_0020("Finished stop transfomer."),
  TRAN_LOG_0021("Finished interrupt all initial transformer threads."),
  TRAN_LOG_0022("Stop transformer time out(waiting mills: {}), force stopping transformer."),
  TRAN_LOG_0023("Thread ready: {}."),
  TRAN_LOG_0024("{}: Starting build avro data, size: {}, schema: {}."),
  TRAN_LOG_0025("{}: Finished build avro data, length: {}, spent time: {} milliseconds."),
  TRAN_LOG_0026("{}: Finished to init bitsflow agents, host: {}, service: {}."),
  TRAN_LOG_0027("{}: Starting to publish avro data to bitsflow."),
  TRAN_LOG_0028("{}: Finished to publish avro data to bitsflow, topic: {}."),
  TRAN_LOG_0029("{}: Starting convert data to xml string, size: {}."),
  TRAN_LOG_0030("{}: Finished convert date to xml string, length: {}, spent time: {} milliseconds."),
  TRAN_LOG_0031("Creating index {} for target collection {}."),
  TRAN_LOG_0032("Initializing transformer config."),
  TRAN_LOG_0033("Initialize transformer config completed"),
  TRAN_LOG_0034("First time run on data script, spent: {} ms, data size: {}"),
  TRAN_LOG_0035("Create {} database index on table: {}, key: {}"),
  TRAN_LOG_0036("Finished create index, spend: {} ms"),


  // Job
  JOB_LOG_0001("Launching job [[{}]]."),
  JOB_LOG_0002("Finished stop transformer runners, saving stats info to database."),
  JOB_LOG_0003("Stopped {}'s transformer."),
  JOB_LOG_0004("Source read paused, queue is full, current queue size: {}"),
  JOB_LOG_0005("Processed event notification, name {}, status {}."),
  JOB_LOG_0006("Stopping transformer runners for {},please wait."),
  JOB_LOG_0007("Thread [ {} ] interrupted"),
  JOB_LOG_0008("Message Consumer interrupted, will retry after 1s."),
  JOB_LOG_0009("Cleared trigger log {}, spent: {} secs, delete rows: {}."),
  JOB_LOG_0010("Finished clear trigger log, spent: {} secs."),
  JOB_LOG_0011("Removed job {}'s message queue and cache."),
  JOB_LOG_0012("Flush job {}'s stats to meta database."),
  JOB_LOG_0013("Removed job {} from job map."),
  JOB_LOG_0014("Stopped job {}'s connector runner."),

  /**
   * DEBUG
   */
  // Connector
  D_CONN_LOG_0001("Add redo logs into logminer size:{} redologs:{}."),
  D_CONN_LOG_0002("Transaction {} has committed, transaction size {}"),
  D_CONN_LOG_0003("Found new transaction {}."),
  D_CONN_LOG_0004("Inserting remaining msgs: {}, message from:{}."),
  D_CONN_LOG_0005("Found running job: {}"),
  D_CONN_LOG_0006("Change stream find a document: {}"),

  // Transformer
  D_TRAN_LOG_0001("New table data found. Waiting for last table to complete."),
  D_TRAN_LOG_0002("Thread {} already exits, threadAck size {}, total threads size {}."),
  D_TRAN_LOG_0003("Received switch upsert mode event, waiting for last thread complete."),

  /**
   * WARN
   */
  // Connector
  W_CONN_LOG_0001("Oplog filter triggered. ns= {}, op= {}, _id= {}, action= {}."),
  W_CONN_LOG_0002("When parsing redo logs: {}, attempt retry {}."),
  W_CONN_LOG_0003("Unrecognized operation type: {}."),
  W_CONN_LOG_0004("Found table [{}] without primary key. Excluding from cluster clone job."),
  W_CONN_LOG_0005("Job [job name: %s] failed to start"),
  W_CONN_LOG_0006("Unsupported database type {}."),
  W_CONN_LOG_0007("CDC is not enabled for Table {}, cannot perform CDC operation."),
  W_CONN_LOG_0008("Unable to find source record by rowid {}. Scanning table to locate the record."),
  W_CONN_LOG_0009("Table [[ {} ] ] missing primary key(s). Incremental changes will not be replicated to target database."),
  W_CONN_LOG_0010("Failed to get snapshot from source data, will retry in 5 seconds.Remain time: {}."),
  W_CONN_LOG_0011("Target database is not replica set or shards, does not support validate feature."),
  W_CONN_LOG_0012("Trying to force stop."),
  W_CONN_LOG_0013("Failed to init sql parser. RuleContextAndOpCode is null. Cannot parse redo sql {}"),
  W_CONN_LOG_0014("Convert table {}'s column value {} column type {} failed {}"),
  W_CONN_LOG_0015("Cannot parse data from redo log {}"),
  W_CONN_LOG_0016("Happened exception {} when mining event. Attempt retry {}"),
  W_CONN_LOG_0017("Cannot parse undo data from redo log {}"),
  W_CONN_LOG_0018("Failed to init sql parser. RuleContextAndOpCode is null. Cannot parse undo sql {}"),

  // Transformer
  W_TRAN_LOG_0001("Failed to process message: {}."),
  W_TRAN_LOG_0002("Relationship {} is not supported."),
  W_TRAN_LOG_0003("Create mongodb index failed, message: {}."),
  W_TRAN_LOG_0004("Failed to bulk write to collection {}, because unique index, total {}, success {}, error {}. Duplicate data: {}."),

  // Job
  W_JOG_LOG_0001("Unable to process event {}, missing executor."),
  W_JOG_LOG_0002("Flush job stats failed {}. No impact on data synchronization, stacks: {}"),

  /**
   * ERROR
   */
  // Connector
  CONN_ERROR_0001("Failed to get redo files, message: {}."),
  CONN_ERROR_0002("Missing schema, job id %s, name %s."),
  CONN_ERROR_0003("Failed to parse redo log content {}, message: {}"),
  CONN_ERROR_0004("Failed to parse redo log {}, message: {}"),
  CONN_ERROR_0005("Failed to read CDC table {}, message: {}, Skip this processing."),
  CONN_ERROR_0006("CDC processing failed, message: {}."),
  CONN_ERROR_0007("Validator failed to monitor target node {}, message: {}, retrying in 3 seconds."),
  CONN_ERROR_0008("Validator failed, message: {}."),
  CONN_ERROR_0009("Failed to process source mysql record: {}, message: {}."),
  CONN_ERROR_0010("Convert record failed. Field: {} Value: {}, message: {}."),
  CONN_ERROR_0011("Failed to process source MongoDB data {}, message: {}."),
  CONN_ERROR_0012("Found unsupported database type: {}."),
  CONN_ERROR_0013("Illegal custom sql: {}, initial offset: {}, message: {}"),
  CONN_ERROR_0014("Failed to read Snapshot from table: {}, message: {}."),
  CONN_ERROR_0015("Failed to start connector runners, message: {}."),
  CONN_ERROR_0016("Failed to connect target mongodb when OneMany lookup, connections: {}, message: {}."),
  CONN_ERROR_0017("Failed to create connection, connections: {}, message: {}."),
  CONN_ERROR_0018("Failed to clear trigger logs, caused by can't found connections, object_id: {}."),
  CONN_ERROR_0019("Change stream put document error, message: {}, event {}"),
  CONN_ERROR_0020("Failed to find job info, job id: {}, message: {}"),
  CONN_ERROR_0021("Failed to read Sybase ASE CDC table {}, message: {}. Will retry after {} seconds."),
  CONN_ERROR_0022("Failed to init Sybase ASE CDC reader, message: {}. Will retry after {} seconds."),
  CONN_ERROR_0023("Failed to convert offset, message: {}."),
  CONN_ERROR_0024("Failed to get sybase date, message: {}."),
  CONN_ERROR_0025("Failed to find Data Rules."),
  CONN_ERROR_0026("Read file {} basic file attributes fail {}"),
  CONN_ERROR_0027("Instance source lib error, message: {}."),
  CONN_ERROR_0028("Database type {} when {} error, need stop: {}, message: {}."),
  CONN_ERROR_0029("Initial job config failed {}, need stop: {}."),
  CONN_ERROR_0030("Failed to force stop job: {}, message: {}."),
  CONN_ERROR_0031("Does not supported {} when use mongodb as source connection."),
  CONN_ERROR_0032("Table(s) %s does not open cdc mode, will stop cdc sync."),
  CONN_ERROR_0033("Init table {} schema failed {}"),
  CONN_ERROR_0034("Unexpected error: {}, will stop replicator."),

  // Transformer
  TRAN_ERROR_0001("Error when transforming, message: {}."),
  TRAN_ERROR_0002("Failed to process node, message: {}."),
  TRAN_ERROR_0003("Bulk operation to target mongoDB failed, collection name: {}, message:  {}."),
  TRAN_ERROR_0004("Transformer job [job name: {}] failed, message: {}."),
  TRAN_ERROR_0005("Bulk operation to target failed, message:  {}."),
  TRAN_ERROR_0006("Stats data processing failed, message: {}."),
  TRAN_ERROR_0007("Process msg {} failed, message: {}."),
  TRAN_ERROR_0008("Failed to drop collections, job will be stop, message: {}."),
  TRAN_ERROR_0009("Run job [id:{}, name:{}] failed, message: {}."),
  TRAN_ERROR_0010("Failed to start connector runners, message: {}."),
  TRAN_ERROR_0011("Transformer process mongodb failed {}"),
  TRAN_ERROR_0012("Bulk writer to target mongodb failed {}, attempt to retry after {}(s) {}"),
  TRAN_ERROR_0013("Bulk writer to collection {} occur exception {}, total {}, success {}, error {}."),
  TRAN_ERROR_0014("Bulk writer to collection {} occur exception {}, attempt to skip this event."),
  TRAN_ERROR_0015("Failed to bulk writer one many data, collection name: {}, message: {}. Attempt to skip to event."),
  TRAN_ERROR_0016("Failed to init avro encoder, message: {}."),
  TRAN_ERROR_0017("Failed to write data in avro will be ignore, message: {}, row data: {}."),
  TRAN_ERROR_0018("Failed to flush encoder, message: {}."),
  TRAN_ERROR_0019("Failed to create avro data file writer, message: {}."),
  TRAN_ERROR_0020("Failed to init bitsflow agents(s), message: %s."),
  TRAN_ERROR_0021("Failed to publish avro data to bitsflow agent(s), message: %s."),
  TRAN_ERROR_0022("Failed to close bitsflow agent(s) session, message: %s."),
  TRAN_ERROR_0023("Failed to build avro data, message: %s, stop on error: %s."),
  TRAN_ERROR_0024("Failed to convert data to xml string, message: %s, stop on error: %s."),
  TRAN_ERROR_0025("XML root name [%s] is invalid, please check in your connection config, example: users.user."),
  TRAN_ERROR_0026("Failed to force stop job: {}, message: {}."),
  TRAN_ERROR_0027("Target value convert error, message: {}."),
  TRAN_ERROR_0028("Bulk writer to collection {} occur exception {}, bulk size {}, skip size {}, remain size {}, operation {}."),
  TRAN_ERROR_0029("Process one many lookup failed {}"),

  // Jobs
  JOB_ERROR_0001("Failed to load connection information for the job[job name: {}]."),
  JOB_ERROR_0002("Failed to prepare job[job name: %s],message: %s"),
  JOB_ERROR_0003("Failed to stop the job[job name: {}], message: {}."),
  JOB_ERROR_0004("Failed to collection job[job name: {}] stats, message: {}."),
  JOB_ERROR_0005("Failed to interrupt job [job name: {}], message: {}."),
  JOB_ERROR_0006("Failed to find stop job list, message: {}."),
  JOB_ERROR_0007("Failed to start running job[%s], message: %s"),
  JOB_ERROR_0008("Failed to paused job if need, job name: [{}], message: {}."),
  JOB_ERROR_0009("Failed job {} target connection schema fail {}."),
  JOB_ERROR_0010("Force stop job id {} name {} failed {}"),


  JOB_WARN_0001("Does not found job source or target connection [ source: {}, target {}], cannot generate job {} target collection schema."),
  JOB_WARN_0002("Does not found job source connection {} schema , cannot generate job {} target collection schema."),
  JOB_WARN_0003("Job {} does not has mappings, cannot generate target collection schema."),
  JOB_WARN_0004("Request management failed when running job {}, {}, will retry next time."),
  JOB_WARN_0005("Request management failed when running dataflow {}, {}, will retry next time."),


  // System
  ERROR_0001("Failed to send email, message: {}."),
  ERROR_0002("Failed to initialize email executor, message: {}."),
  ERROR_0003("Failed to process jobs event, message: {}."),
  ERROR_0004("Failed to enqueue, message: {}."),
  ERROR_0005("Failed to stats job progress, message: {}, will retry after 30 seconds."),
  ERROR_0006("Failed to call rest api, msg %s."),
  ERROR_0007("Found another agent already exists, please make sure that only one agent is running at the same time. This agent will exit...\n"
    + "If already stopped all agent, please wait for one minute and retry."),
  ERROR_0008("Failed to stats job {} progress, message: {}, will retry after 30 seconds."),


  WARN_0001("Job listener listening meta db Job collection failed {}, will retry after 3(s)."),

  // Data rule processor
  ERROR_DRP_0001("Failed to read mapping rule：{}, message: {}."),

  // Memory Queue
  ERROR_MQ_0001("Value converter error, ignore this raw, message: {}\n  field : value = {} : {}, {} - to - {}"),
  ERROR_MQ_0002("Source data convert to List error, message: {}"),
  ERROR_MQ_0003("Source data convert to MessageEntity error, message: {}"),
  ERROR_MQ_0004("Source data values converter error, message: {}"),
  ERROR_MQ_0005("Source data value converter error, message: {}"),

  /**
   * Sybase ASE
   */
  // Info
  SYB_INFO_0001("Finished to {}, execute sql:\r\n{}"),
  SYB_INFO_0002("Created new login, user: {}, password: {}."),
  SYB_INFO_0003("For security, tapdata will lock login: {}, you can execute below sql to unlock: {}."),
  SYB_INFO_0004("Detected that login tp_cdc already exists."),

  // Error
  SYB_ERROR_0001("Failed to check {}, message: {}, sql:\r\n{}"),
  SYB_ERROR_0002("Failed to {}, message: {}, if caused by permission denied, please contact your DBA to execute below sql:\r\n{}"),
  SYB_ERROR_0003("Failed to check {}, message: {}."),
  SYB_ERROR_0004("Failed to read CDC record table, message: {}, try to contact your DBA and execute below sql: \r\n{}"),
  SYB_ERROR_0005("Failed to find CDC trigger, message: {}, if caused by permission denied, please contact your DBA to execute below sql:\r\n{}"),
  SYB_ERROR_0006("Failed to create trigger, message: {}, if caused by permission denied, please contact your DBA to execute below sql:\r\n{}"),
  SYB_ERROR_0007("Failed to delete from table {}, message: {}, if caused by permission denied, please contact your DBA to execute below sql: \r\n{}"),
  SYB_ERROR_0008("Failed to clear trigger logs when read sysobjects from catalog {}, message: {}, if caused by permission denied, please contact your DBA to execute below sql: \r\n{}"),
  SYB_ERROR_0009("Failed to check cdc tables, message: {}, if caused by permission denied, please contact your DBA to execute below sql: \r\n{}"),
  SYB_ERROR_0010("Failed to build create table sql, message: {}."),
  SYB_ERROR_0011("Execute sql error: {}"),
  SYB_ERROR_0012("Failed to create index on cdc log table, message: {}, please execute it again by manual operation, sql: \r\n{}"),


  /**
   * processor
   */
  PROCESSOR_ERROR_0001("Unique key/Array unique key {} must not be removed"),
  PROCESSOR_ERROR_0002("Cannot convert data type {} to {}."),
  PROCESSOR_ERROR_0003("Cannot convert { data: {}, data type: {} } to {}."),
  PROCESSOR_ERROR_0004("Script process record {} failed {} and stop on error is true, will stop replicator."),
  PROCESSOR_ERROR_0005("Script process record {} error: {}"),

  /**
   * gridfs
   */
  GRIDFS_ERROR_0001("Failed to upload file {}, message: {}."),
  GRIDFS_ERROR_0002("File does not exists: {}"),
  GRIDFS_ERROR_0003("Failed to scan file in folder: {}, message: {}."),

  GRIDFS_INFO_0001("Starting scan file from folder: {}."),
  GRIDFS_INFO_0002("Finished upload all file(s) into gridfs, folder: {}, database: {}."),
  GRIDFS_INFO_0003("Uploaded file: {}."),

  /**
   * log collector
   */
  LOG_WARN_0001("Log collector failed to start, mapping is empty: %s"),


  /**
   * data validate
   */

  DATA_VALIDATE_ERROR_0001("Generate validate data failed {}"),

  /**
   * Custom connections
   */
  CUSTOM_CONN_ERR_0001("Run on data script error: {}."),
  ;

  private final String msg;

  TapLog(String msg) {
    this.msg = msg;
  }

  public String getMsg() {
    return msg;
  }
}
