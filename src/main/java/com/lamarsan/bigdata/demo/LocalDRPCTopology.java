package com.lamarsan.bigdata.demo;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.drpc.LinearDRPCTopologyBuilder;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * className: LocalDRPCTopology
 * description: TODO
 *
 * @author lamar
 * @version 1.0
 * @date 2020/1/26 19:44
 */
public class LocalDRPCTopology {
    public static class MyBolt extends BaseRichBolt {

        private OutputCollector outputCollector;

        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.outputCollector = collector;
        }

        @Override
        public void execute(Tuple input) {
            // 请求id
            Object requestId = input.getValue(0);
            // 请求参数
            String name = input.getString(1);

            String result = "add user:" + name;
            this.outputCollector.emit(new Values(requestId, result));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("id", "result"));
        }
    }

    public static void main(String[] args) {
        LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder("addUser");
        builder.addBolt(new MyBolt());
        LocalCluster localCluster = new LocalCluster();
        LocalDRPC drpc = new LocalDRPC();
        localCluster.submitTopology("local-drpc", new Config(), builder.createLocalTopology(drpc));
        String result = drpc.execute("addUser", "zhangsan");
        System.err.println("From client:" + result);
        localCluster.shutdown();
        drpc.shutdown();
    }
}
