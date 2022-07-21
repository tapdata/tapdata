## Oracle config

### 1.oracle database log mode config

- select oracle database log mode Run following command:

      ARCHIVE LOG LIST;

- if log mode is `NOARCHIVELOG`, change the log mode Run following command:

      ALTER SYSTEM SET log_archive_dest="D:\oracle\oradata\practice\ARCHIVE";

  `practice` is your database name, `D:\oracle\oradata\` is your install directory

- alter the log archive format Run following command:

      ALTER SYSTEM SET log_archive_format="ARC%S_%R.%T" SCOPE=SPFILE;

- restart database, Run following command:

      SHUTDOWN IMMEDIATE;
      STARTUP MOUNT;
      ALTER DATABASE ARCHIVELOG;

- if restart success , log mode is archivelog mode

  check work Run following command:

      SELECT dest_id, status, destination FROM v$archive_dest WHERE dest_id =1;
- open database Run following command:

      ALTER DATABASE OPEN;

### 2.Oracle logminer config:

- create the logminer folder `LOGMNR`, exmple `D:\oracle\oradata\practice\LOGMNR`

- set the logminer dictionary path Run following command:

      CREATE DIRECTORY utlfile AS 'D:\oracle\oradata\practice\LOGMNR';
      ALTER system set utl_file_dir='D:\oracle\oradata\practice\LOGMNR' scope=spfile;

- open the supplemental log mode of logminer Run following command:

      ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;

- restart database instance Run following command:

      SHUTDOWN IMMEDIATE;
      STARTUP;
      SHOW PARAMETER utl_file_dir;

- create logminer user `LOGMINER`, grant for DBA role Run following command:

      CREATE USER LOGMINER IDENTIFIED BY LOGMINER;
      GRANT CONNECT, RESOURCE,DBA TO LOGMINER;

### 3.build

- if your local maven repository have not ojdbc6.jar oracle driver, need to build on local, Run following command:

      shell> wget http://www.datanucleus.org/downloads/maven2/oracle/ojdbc6/11.2.0.3/ojdbc6-11.2.0.3.jar
      shell> mvn install:install-file -DgroupId=com.oracle -DartifactId=ojdbc6 -Dversion=11.2.0.3 -Dpackaging=jar -Dfile=ojdbc6-11.2.0.3.jar -DgeneratePom=true


- Run following command:

      cd tapdata/embedded/
      mvn clean package
- start up docker compose, Run following command:

      cd tapdata
      docker-compose up -d

## Setting up MySQL

- Enabling the binlog. add follow text in /etc/mysql/my.cnf. Restart mysqlId

      server-id         = 223344
      log_bin           = mysql-bin
      binlog_format     = row
      expire_logs_days  = 10

- Create a MySQL user for the Redoma

      SQL> GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'debezium' IDENTIFIED BY 'dbz';




