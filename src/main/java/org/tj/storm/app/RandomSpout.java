package org.tj.storm.app;


import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;
import java.util.Random;

/**
 * storm数据产生
 * Created by tangjing on 2019/10/21.
 */
public class RandomSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    public static final String[] words = new String[]{"apple test", "orange hello", "world pineapple", "banana", "watermelon"};

    public void nextTuple() {
        Random random = new Random();
        String word = words[random.nextInt( words.length )];
        this.collector.emit( new Values( word ) );

        System.out.println( "emit: " + word );

        Utils.sleep( 500 );
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare( new Fields( "line" ) );
    }


}
