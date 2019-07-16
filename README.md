# diff_schema
MySQL 数据库表结构对比工具

Forked 自 [napoleonu/merge_schema](https://github.com/napoleonu/merge_schema)，根据实际使用情况进行优化修改。

## 使用命令

1. 使用 SQL 文件对比

`python diff_schema.py -d file -s source.sql -t target.sql -o diff.sql`

2. 连接数据库对比

`python diff_schema.py -d db -s root:root@127.0.0.1:3306~dbname1 -t root:root@127.0.0.1:3306~dbname2 -o diff.sql`

## 注意事项

- 表设置信息修改只检查 `ENGINE`, `CHARSET`, `COMMENT`的改变来做对比
- 使用 SQL 文件对比请确保源文件和目标文件都是使用相同一个工具导出