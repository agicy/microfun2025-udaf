# microfun2025-udaf

这个仓库包含了 2025 年北京邮电大学柠檬微趣奖学金课题——基于 Hive 框架的自定义聚合函数实现的相关代码和报告。

## 课题背景

在海量用户行为分析场景中，需要高效地统计每个用户的「总活跃时长」。给定一系列用户活跃时间点的时间戳，如果任意两个时间点的间隔小于等于一个预设的阈值（600 秒），则认为这两个时间点属于同一个连续的活跃区间。本项目的目标是设计并实现一个高效的 Apache Hive **自定义聚合函数**（**U**ser-**D**efined **A**ggregate **F**unctions，**UDAF**），用于计算每个用户所有活跃区间的总时长。

## 项目结构

```
.
├── docs                                # 报告文档
├── pom.xml                             # Maven 配置文件
└── src
    ├── main
    │   └── java/com/microfun/udaf
    │       ├── DummyGetOnlineDurationUDAF.java  # 基准实现的 UDAF
    │       └── BlockGetOnlineDurationUDAF.java  # 优化后的 UDAF
    └── test
        ├── java/com/microfun/udaf        # UDAF 单元测试代码
        ├── sql                           # 生成测试数据的 SQL 脚本
        └── util                          # 处理性能测试结果的 Python 测试脚本
```

## 使用说明

1.  编译项目并打包成 jar 文件。
2.  将 jar 文件添加到 Hive 的 classpath 中。
3.  在 Hive 中创建函数：

```sql
CREATE FUNCTION get_online_duration AS 'com.microfun.udaf.BlockGetOnlineDurationUDAF';
```

4.  之后即可在 SQL 查询中使用该 UDAF。
