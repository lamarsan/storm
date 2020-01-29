package com.lamarsan.bigdata.integration.kafka;

import com.lamarsan.bigdata.utils.DateUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * className: LogProcessBolt
 * description: 接受Kafka的数据进行处理
 *
 * @author lamar
 * @version 1.0
 * @date 2020/1/26 20:56
 */
public class LogProcessBolt extends BaseRichBolt {
    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        try {
            byte[] binaryByField = input.getBinaryByField("bytes");
            String value = new String(binaryByField);
            String[] splits = value.split("\t");
            String imsi = splits[0];
            String longitude = splits[1];
            String latitude = splits[2];
            long time = DateUtils.getInstance().getTime(splits[3]);
            collector.emit(new Values(time, Double.parseDouble(longitude), Double.parseDouble(latitude)));
            System.out.println(imsi + "," + longitude + "," + latitude + "," + time);
            this.collector.ack(input);
        } catch (Exception e) {
            this.collector.fail(input);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("time", "longitude", "latitude"));
    }
}
