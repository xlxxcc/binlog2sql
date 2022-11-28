# binlog2sql
binlog2sql工具的java版

## 功能
* 本地读取binlog日志文件生成sql或反向sql
* slave流读取binlog日志生成sql或反向sql
* 输出query.dml,方便查找回滚起始position
* 处理bit值为{0}问题

## 回滚规则:
delete -> insert
insert -> delete
update -> update

## 参照
binlog的读取和解析工具: https://github.com/shyiko/mysql-binlog-connector-java

参考了 Python版的binlog2sql: https://github.com/danfengcao/binlog2sql
参考了 Java版的binlog2sql: https://github.com/xin497668869/binlog2sql_java