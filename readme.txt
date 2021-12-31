branch1.0: 稳定版本，在测试环境可以正常运行。
存在的bug：多了些注释，多配了不需要的环境变量，namespace好像没有指定

branch1.1:
做出的更新：将多余的注释去掉。新建一个项目入口Bootstrap2用于测试在指定namespace下创建表。
存在的bug：多配的环境变量没有去掉，当concat字段存在null值时，rowkey就会被置为null。第一会导致空指针，
第二会导致插入的那批hfile中有相同rowkey的记录

branch1.2:
做出的更新：修改上一个版本concat字段存在null值的bug。所有字段用nvl包括起来，如为空值就用"null"代替。前提是只指定
String类型的字段为rowkey。如果传入了非空的Int类型不会报错，理论上说源表中为Int类型的字段应该不会为null
存在的bug：份额历史表理论上根据七个字段可以确定唯一的值，但实际操作不是如此。pd_code为519999的产品，可能会把不同
的基金账号和交易账号映射为相同的。  连接rowkey，各个字段之间要用-连接。  代码中需要判断namespace是否存在，不在就要自己创建

branch1.3:
做出的更新：组成rowkey的各个字段之间用-连接。
存在的bug：代码中需要判断namespace是否存在，不在就要自己创建。   适配历史份额表存在重复数据的bug

branch1.4:
做出的更新：删除多余配置。自动创建namespace。只保留一份程序入口。第一个参数改为hbase.
存在的bug：适配历史份额表存在重复数据的bug。map阶段存在数据侵倾斜。map阶段一个stage可能会执行多次。