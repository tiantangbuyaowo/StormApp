package org.tj.storm.app;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by tangjing on 2019/10/22.
 */
public class CountBolt extends BaseBasicBolt {
    public static final Map<String, AtomicInteger> map = new HashMap<String, AtomicInteger>();

    /**
     * 业务逻辑：
     * 1）获取每个单词
     * 2）对所有单词进行汇总
     * 3）输出
     */
    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        // 1）获取每个单词
        String word = input.getStringByField( "word" );
        AtomicInteger count = map.get( word );
        if (count == null) {
            count = new AtomicInteger( 0 );
        }

        count.incrementAndGet();

        // 2）对所有单词进行汇总
        map.put( word, count );

        // 3）输出
        collector.emit( new Values( word, map.get( word ) ) );
        System.out.println( word + ":" + count );

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare( new Fields( "word", "count" ) );
    }
}
