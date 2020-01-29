package com.lamarsan.bigdata.demo;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;

/**
 * className: LocalSumStorm
 * description: 使用storm实现累加
 *
 * @author lamar
 * @version 1.0
 * @date 2020/1/23 20:20
 */
public class LocalSumStorm {
    public static class DataSourceSpout extends BaseRichSpout {
        private SpoutOutputCollector collector;

        /**
         * 初始化
         */
        @Override
        public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
            this.collector = spoutOutputCollector;
        }

        int number = 0;

        /**
         * 产生数据，从消息队列中获取
         * <p>
         * 死循环，一直执行
         */
        @Override
        public void nextTuple() {
            this.collector.emit(new Values(number++));
            System.out.println("Spout:" + number);
            Utils.sleep(1000);
        }

        /**
         * 声明输出字段
         */
        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("num"));
        }
    }

    /**
     * 数据累计求和
     */
    public static class SumBolt extends BaseRichBolt {
        /**
         * 初始化
         */
        @Override
        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        }

        int sum = 0;

        /**
         * 也是一个死循环
         * 获取spout发出的数据
         */
        @Override
        public void execute(Tuple tuple) {
            // 可以根据index获取，也可以根据field名称获取
            Integer value = tuple.getIntegerByField("num");
            sum += value;
            System.out.println("Bolt:sum=[" + sum + "]");
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        }
    }

    public static void main(String[] args) {
        // 根据Spout和Bolt来构建出Topology
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("DataSourceSpout", new DataSourceSpout());
        builder.setBolt("SumBolt", new SumBolt()).shuffleGrouping("DataSourceSpout");
        // 本地模式
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("LocalSumStormTopology", new Config(), builder.createTopology());

    }
}
