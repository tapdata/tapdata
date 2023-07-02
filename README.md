<p align="center">
    <img src="https://github.com/tapdata/tapdata-private/raw/master/assets/logo-orange-grey-bar.png" width="300px"/>
</p>
<p align="center">
    Tapdata is a Real Time data integration platform that enables data to be synchronized in real-time among various systems such as databases, SaaS services, applications, and files.
</p>

<p align="center">
    <a href="https://github.com/tapdata/tapdata/graphs/contributors" alt="Contributors">
    <img src="https://img.shields.io/github/contributors/tapdata/tapdata" /></a>
    <a href="https://github.com/tapdata/tapdata/pulse" alt="Activity">
    <img src="https://img.shields.io/github/commit-activity/m/tapdata/tapdata" /></a>
    <a href="https://tapdata.github.io/tapdata">
    <img src="https://github.com/tapdata/tapdata/actions/workflows/build.yml/badge.svg" alt="build status"></a>
</p>

Not satisfied with data synchronization delays of several minutes, Tapdata platform is specifically designed for **Real Time** data integration. Based on a log parsing solution, it reduces data synchronization latency to **few seconds**, while offering a visual task builder. Enjoy your real-time data journey now with Tapdata !

![](./assets/mysql-mongodb-monitor.png)
**screenshot from <a href="https://cloud.tapdata.io/">Tapdata Cloud Service</a>**

### Cloud Service: https://cloud.tapdata.io/
 
## Install
1. click https://cloud.tapdata.io/, start your real-time data journey immediately, Free Trial Available(**NO credit card required**)
2. you can easily deploy the service in your local environment with 2 Steps:
    1. Install Docker
    2. Exec: `docker run -itd -p 3030:3030 github.com/tapdata/tapdata-opensource:latest bash`, then you can get tapdata service by visting http://localhost:3030,
    default username is: `admin@admin.com`, default password is `admin`

### Example Usage
#### Sync Data From Mysql To MongoDB
1. Add Mysql Connection
![](./assets/mysql-conn.png)

2. Add MongoDB Connection
![](./assets/mongodb-conn.png)

3. Click Data Pipelines => Replications, Create a new job

4. Drag Mysql Connection and MongoDB Connection from left side bar, Drag a line from the MySQL connection node and connect it to the MongoDB node
5. Click Mysql Node, select tables what you want to sync
![](./assets/mysql-mongodb-config.png)

6. Start Job and watch data keep syncing!
![](./assets/mysql-mongodb-monitor.png)


## License
Defaults to Server Side Public License.

For Connectors, license is Apache V2.

## Join now
- [Join slack channel](https://join.slack.com/t/tapdatacommunity/shared_invite/zt-1biraoxpf-NRTsap0YLlAp99PHIVC9eA)
- [Send email to us](mailto:cloud@tapdata.io)
