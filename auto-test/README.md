# Tapdata 接口集成测试工具使用文档
本工具用来对已经部署的 Tapdata 服务进行集成测试

## 准备工作
1. 自行安装 python3, pip3
2. 安装依赖: `pip install -r requirements.txt`

## 配置
1. 配置环境地址: 可以通过环境变量 `server` 指定, 也可以编写 `config.yaml` 赋值

## 运行
### 默认用例
1. 执行 `bash test.sh` 运行默认用例

### 详细用例
1. 执行 `python3 init/prepare_data.py` 导入数据
2. 执行 `python3 init/create_database.py` 在 Tapdata 服务端创建数据源
3. 执行 `python3 cases/runn.py -h` 查看用例执行帮助, 其帮助如下:
```
python3 runner.py --help
login tapdata with access_code success!
please type h get global help          
usage: runner.py [-h] [--case CASE] [--source SOURCE] [--sink SINK] [--bench BENCH] [--smart_cdc] [--clean] [--core] [--nowait]

optional arguments:
  -h, --help       show this help message and exit
  --case CASE      test case file, choose it from test_.* from this path
  --source SOURCE  datasource name, if None, will run all possible datasources
  --sink SINK      data sink name, if None, will run all possible sinks
  --bench BENCH    bench cdc event number
  --smart_cdc      use a tapdata job, make data load and cdc for all support datasource
  --clean          clean all data/datasource/job after case finish
  --core           only run core datasource
  --nowait         just start job, dont wait for it finish
```

## 扩展
### 增加测试数据源
修改 `config.yaml`, 增加配置即可, 其配置格式如下:
```
qa_mysql: // 数据源名称
  connector: "mysql" // 连接器类型, 与 PDK 定义保持一致
  host: "******"
  port: 3306
  username: "***"
  password: "***"
  database: "database" // 一些数据源配置, 与 PDK 定义保持一致
  __type: ["source", "sink", "cdc"] // 声明是可以作为源, 还是目标, 支不支持实时增量
  __core: false // 声明是否属于核心数据源
```

### 增加测试用例
在 cases 下新建一个以 test_ 开头的 py 文件, 其中创建名为 test 的方法, 支持的参数有:
1. p: 会传入已经创建好的 pipeline 对象
2. pipeline: 传入 Pipeline 类, 可自行创建 pipeline 对象
3. 数据表: 各种测试环境提供的数据表, 支持以下形式:
   1. 不限定数据源类型的数据来源表: 以 `int/data` 目录下的文件, 文件名形式的, 比如 `car_claim`, 
   2. 限定数据源类型的数据来源表: 在上述基础上, 增加 config.yaml 里的数据源配置前缀, 比如 `qa_mongodb_car_claim`
   3. 数据目标: 在上述 1/2 的基础上, 增加 _sink 后缀

如果需要支持自定义任务检查, 可创建一个名为 check 的方法, 参数同上, 此方法返回 True 标明自定义检查通过, False 表明自定义检查失败