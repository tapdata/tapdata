# Building Guide
Welcome to the project build guide. This guide will help you compile and run the project successfully on your local machine.

## Prerequisites
Before you begin, ensure that your system has the following software and tools installed:
1. git
2. node v16.14.2 or v16 version higher
3. jdk 1.8, or 11, and mvn installed
4. bash
5. network is available

building is only tested in linux and mac, if you want to build tapdata in other system, please try it, thanks to report bug to us if you find some problems

### Building Guidelines
build all libs and package from source using command:

```
p=`pwd`
rm -rf ~/.m2/repository/com/tapdata/
rm -rf ~/.m2/repository/io/tapdata/
mkdir -p $p/build
cd $p/build
git clone git@github.com:tapdata/tapdata-web.git
git clone git@github.com:tapdata/tapdata.git
git clone git@github.com:tapdata/tapdata-connectors.git
git clone git@github.com:tapdata/tapdata-common-lib.git

rm -rf $p/build/tapdata/iengine/ie.jar
rm -rf $p/build/tapdata/manager/tm/target/tm-0.0.1-SNAPSHOT-exec.jar
rm -rf $p/build/tapdata-web/dist

cd tapdata-common-lib
git fetch
git checkout -f
git checkout main
git rebase
git pull --rebase
echo "building tapdata-common-lib..."
mvn clean install -DskipTests
if [[ $? -ne 0 ]]; then
    echo "build tapdata-common-lib failed, please check logs to get cause"
    exit 1
fi

cd $p/build
cd tapdata-web
git fetch
git checkout -f
git checkout develop
git rebase
git pull --rebase
echo "building frontend..."
bash build/build.sh
if [[ $? -ne 0 ]]; then
    echo "build frontend failed, please check logs to get cause"
    exit 1
fi

cd $p/build
cd tapdata
git fetch
git checkout -f
git checkout develop
git rebase
git pull --rebase
echo "building engine..."
mvn clean install -DskipTests -U
if [[ $? -ne 0 ]]; then
    echo "build engine failed, please check logs to get cause"
    exit 1
fi

cd $p/build
cd tapdata-connectors
git fetch
git checkout -f
git checkout develop
git rebase
git pull --rebase
echo "building connectors..."
mvn clean install -DskipTests -U
if [[ $? -ne 0 ]]; then
    echo "build connectors failed, please check logs to get cause"
    exit 1
fi
```

If everything is ok, you will get:
1. engine jar: build/tapdata/iengine/ie.jar
2. manager jar: build/tapdata/manager/tm/target/tm-0.0.1-SNAPSHOT-exec.jar
3. webui: build/tapdata-web/dist

### Start Guidelines

If you want to start the complete Tapdata service on your machine, please follow these steps:
### Instructions for Starting the Complete Tapdata Service on Your Machine

1. **Deploy a MongoDB Replica Set and Record the Access URI**:
    - Set up a MongoDB replica set and make a note of the URI that you will use to access it.

2. **Create a `webroot` Directory and Copy WebUI Files**:
    - In the directory where the management JAR file is located, create a `webroot` directory.
    - Copy all files from the `dist` directory of the WebUI into the `webroot` directory.

3. **Set Environment Variables**:
    - Set some environment variable. This URI is your MongoDB replica set connection string:
    
        ```sh
        export mongodb_uri='mongodb://127.0.0.1:27017/tapdata'
        export TAPDATA_MONGO_URI=$mongodb_uri
        export app_type='DAAS'
        export backend_url='http://127.0.0.1:3030/api/'
        export TAPDATA_WORK_DIR=<path-to-your-management-directory>
        ```

4. **Start the Management Service**:
    - Run the following command to start the management service:

        ```sh
        java -jar -Dserver.port=3030 -server -Xmx2G tm.jar -Dspring.data.mongodb.default.uri=${mongodb_uri} -Dspring.data.mongodb.log.uri=${mongodb_uri} -Dspring.data.mongodb.obs.uri=${mongodb_uri}
        ```

5. **Start the Engine**:
    - Run the following command to start the engine:

        ```sh
        java -Xmx2G -jar components/ie.jar
        ```

### HA Deployment
The open-source version supports all high-availability features of the enterprise version, but it does not support visual multi-node management. You can follow the steps below to deploy a high-availability cluster.

#### Meta Database
Tapdata uses MongoDB as the metadata repository. You can deploy a MongoDB replica set with more than three nodes to provide high availability for the metadata database.

#### Manager
Tapdata Manager is a stateless node. If you need high availability for this component, simply start multiple instances on different machines.

Please note that if you deploy multiple Manager nodes, when starting the engine, you need to add the addresses of all Manager nodes to `backend_url`, separating multiple addresses with commas.

#### Engine
You only need to deploy multiple engines on different machines. In the event of an engine failure, tasks will automatically transfer between different engines. You may observe that some tasks have no progress for a period, but they will automatically resume. The default high availability switch time is 10 minutes, which we understand to be an appropriate value. This ensures that tasks can switch in a timely manner when a failure occurs and avoids unnecessary task migration due to network fluctuations.

By following these steps, you can start the complete Tapdata service on your machine.
