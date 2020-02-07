package com.lamarsan.bigdata.integration.kafka;

import com.google.common.collect.Maps;
import com.lamarsan.bigdata.demo.ClusterSumStormTopology;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.jdbc.bolt.JdbcInsertBolt;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.common.HikariCPConnectionProvider;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcMapper;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.topology.TopologyBuilder;

import java.util.Map;
import java.util.UUID;

/**
 * className: StormKafkaTopo
 * description: kafka整合storm
 *
 * @author lamar
 * @version 1.0
 * @date 2020/1/26 20:53
 */
public class StormKafkaTopo {
    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        BrokerHosts hosts = new ZkHosts("localhost:2181");
        String topic = "project_topic";
        String zkRoot = "/" + topic;
        String id = UUID.randomUUID().toString();
        SpoutConfig spoutConfig = new SpoutConfig(hosts, topic, zkRoot, id);
        // 每次只读最新的
        spoutConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
        String SPOUT_ID = KafkaSpout.class.getSimpleName();
        builder.setSpout(SPOUT_ID, kafkaSpout);
        String BOLD_ID = LogProcessBolt.class.getSimpleName();
        builder.setBolt(BOLD_ID, new LogProcessBolt()).shuffleGrouping(SPOUT_ID);

        Map hikariConfigMap = Maps.newHashMap();
        hikariConfigMap.put("dataSourceClassName","com.mysql.jdbc.jdbc2.optional.MysqlDataSource");
        hikariConfigMap.put("dataSource.url", "jdbc:mysql://localhost/software_data_clean");
        hikariConfigMap.put("dataSource.user","root");
        hikariConfigMap.put("dataSource.password","root");
        ConnectionProvider connectionProvider = new HikariCPConnectionProvider(hikariConfigMap);

        String tableName = "stat";
        JdbcMapper simpleJdbcMapper = new SimpleJdbcMapper(tableName, connectionProvider);

        JdbcInsertBolt userPersistanceBolt = new JdbcInsertBolt(connectionProvider, simpleJdbcMapper)
                .withTableName(tableName)
                .withQueryTimeoutSecs(30);

        builder.setBolt("JdbcInsertBolt", userPersistanceBolt).shuffleGrouping(BOLD_ID);

        // 提交到storm集群
        String topoName = StormKafkaTopo.class.getSimpleName();
        try {
            StormSubmitter.submitTopology(topoName, new Config(), builder.createTopology());
        } catch (AlreadyAliveException e) {
            e.printStackTrace();
        } catch (InvalidTopologyException e) {
            e.printStackTrace();
        } catch (AuthorizationException e) {
            e.printStackTrace();
        }
        // 本地运行
        //LocalCluster cluster = new LocalCluster();
        //cluster.submitTopology(StormKafkaTopo.class.getSimpleName(), new Config(), builder.createTopology());
    }
}
