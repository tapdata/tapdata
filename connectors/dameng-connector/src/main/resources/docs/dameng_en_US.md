## **连接配置帮助**
### **1. DM 安装说明**
请遵循以下说明以确保在 Tapdata 中成功添加和使用DM数据库，注意：DM 实时同步基于DM Redo Log，因此需要提前执行某些配置。
### **2. 先决条件（作为源）**
#### **2.1 开启 LogMiner**
- 以具有 DBA 权限的用户身份登录数据库
- 查看数据库的是否开启归档以及归档日志 :`select para_name, para_value from v$dm_ini where para_name in ('ARCH_INI','RLOG_APPEND_LOGIC');`
  ARCH_INI 归档日志 0为未开启 1为开启
- 开启操作：
- ALTER DATABASE MOUNT;
- ALTER DATABASE ADD ARCHIVELOG 'TYPE=LOCAL,DEST=/bak/archlog,FILE_SIZE=64,SPACE_LIMIT=1024';
- ALTER DATABASE ARCHIVELOG;
- ALTER DATABASE OPEN;

  设置日志地址参数:
  - DEST:
    归档文件存放目录(本地/远程), 如果所指向的本地目录不存在会自动创建.
  - TYPE=归档类型,  :
    远程实时归档(REALTIME)
    远程异步归档(ASYNC)
    远程同步归档(SYNC)
    本地归档(LOCAL)
    MPP远程归档(MARCH)
    FILE_SIZE=归档文件大小,space_limit=空间大小限制

- RLOG_APPEND_LOGIC ,附加日志:
-  0	不启用
   1	如果有主键列，记录update和delete操作时只包含主键列信息，若没有主键列则包含所有列信息
   2	不论是否有主键列，记录update和delete操作时都包含所有列的信息
   3	记录update时包含更新列的信息以及rowid，记录delete时只有rowid
- 建议附加日志设置1
- alter system set 'RLOG_APPEND_LOGIC'=1 MEMORY;




