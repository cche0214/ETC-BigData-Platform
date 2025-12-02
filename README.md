# ETC-BigData-Platform
Highway ETC big data management and monitoring system built with Hadoop, Spark, Flink, and HBase.

# 2025-11-30 项目规划
# 1. 初步探索
#   1.1 数据预处理：原始的数据很脏，每个月份的数据名称不一样，每个数据格式内部的内容也很乱，第一次只是简单清洗，应该根据需要对字段进行严格的清洗，这个需要确定。
#   1.2 数据存储：现在的做法是简单清理数据得到12月和1月的两张大表，然后我只实现了用KafKa把每一条读入消息队列，然后Flink放入Hbase里面，Hbase的存储逻辑是需要设计row-key的，我目前设计的row-key不是很能达到我的要求，这个需要讨论；同时也可以用Flink放到MyCat中间件，存到MySQL里面，这里需要设计分库分表，需要讨论；最后是用Flink放到Redis里面，这个我没学过，可以再多了解一下，但是它可以用来做实时大屏。
#   1.3 接口：目前我的电脑三台虚拟机node1 node2 node3的ip分别是192.168.88.131 192.168.88.132 192.168.88.133，然后我的电脑设置了域名映射可以ping通，其他人的电脑行不行？Hbase提供thrift接口，监听在8085，是给Flask调用Hbase使用的，前端用Vue对接Flask在我本地的8080接口来访问，这是目前实现的。然后就是MySQL我测试了把数据简单放在node1上面，通过node1的3306接口来访问，可以实现SQLAgent，当然这个后期要加强内容。
# 2. 项目需求
#   2.1 交互式访问：设置好MyCat的分片规则之后，KafKa生产数据，Flink接入MyCat然后根据规则把数据写入MySQL之后，通过langchain SQLAgent调用DeepSeek api来实现交互式查询。
#   2.2 可视化大屏，动态展示实时信息，要求每半分钟一次对结果进行刷新：这个是说用Redis内存数据库来实现，但是我暂时还没了解，也可能可以用其他的。
#   2.3 实时套牌车检测：这个的实现形式还没有考虑，不知道放在哪一个部分，实验给的例子是用Flink来计算分析，然后交给前端报警
#   2.4 车流预警算法，对收费站车流进行实时预警并显示：这个考虑是用CNN+LSTM+Attention来实现预测，就是预测HBase里面的数据，当然是要预处理的，所以在实现算法之前要看看需要哪些字段。
# 3. 一些其他问题
#   3.1 给了一个CloudLab，其实就是国外的一些高性能计算机组成的集群，我们可以租用，但是在上面需要重新配置Hadoop Flink啥的，我就没搞，目前都是用的自己的虚拟机，然后去年他们用的是华为云的ECS？这个我不了解，没用过

# 目前的一些启动指令：
#   在 node1, node2, node3 分别执行:
#       zkServer.sh start kafka-server-start.sh -daemon $KAFKA_HOME/config/server.properties
#   (在 NameNode 节点，通常是 node1)：
#       start-dfs.sh start-yarn.sh  start-hbase.sh hbase-daemon.sh start thrift -p 8085
#   KafKa相关：
#   kafka-topics.sh --list --bootstrap-server node1:9092    查看有哪些主题
#   kafka-topics.sh --describe --bootstrap-server node1:9092 --topic etc-traffic-data   查看特定主题的分区和副本状态
#   kafka-console-consumer.sh --bootstrap-server node1:9092 --topic etc-traffic-data --from-beginning --max-messages 10 查看历史数据
# 目前的停止指令：
#   kafka-server-stop.sh hbase-daemon.sh stop thrift stop-hbase.shstop-yarn.sh stop-dfs.sh zkServer.sh stop
