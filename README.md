# 信贷风险控制课程作业的图数据库内容

# pyProject中的文件

graphService: 前端展示用数据返回，具体返回内容查看test code中的graphTest

graphEva: 为person实体创建分析后得知的隐藏关系 + 生成并导出图数据库中可统计数据，供建模使用

graphCU: 实现graph的 增加/更新 节点/关系 

记得更改neo4j密码为本机的密码，默认为neo4j

# 导出图数据库中可统计的数据的使用

仅导出了10000条数据

![image](https://github.com/OnikoKane/RMgraph/blob/master/%E5%AF%BC%E5%87%BA%E7%BB%9F%E8%AE%A1%E6%95%B0%E6%8D%AEx.jpg)

反欺诈model的使用：将label为拒绝的数据后，添加该数据中flag=1的数据同时还需更改personID和txID，label为不拒绝则反之(更改txID是方便后端查询)

评分model的使用：根据label与该数据中的status(rank D -> rank AA)进行匹配，同时需要更改personID和txID(更改txID是方便后端查询)
