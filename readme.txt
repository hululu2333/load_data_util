branch1.0: 稳定版本，在测试环境可以正常运行。但存在一些不足，如：多了些注释，多配了不需要的环境变量，
不能指定hfile的分区个数，namespace好像没有指定

branch1.1: 将多余的注释去掉。新建一个项目入口Bootstraps用于测试在指定namespace下创建表。多配的环境变量没有去掉，
hfile文件个数通过执行spark-submit时手动指定