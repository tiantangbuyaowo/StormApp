package org.tj.storm.app;


import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * Created by tangjing on 2019/10/21.
 */
public class SplitBolt extends BaseBasicBolt {


    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String word = input.getStringByField( "line" );
        for (String key : word.split( " " )) {
            collector.emit( new Values( key ) );
        }

    }


    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare( new Fields( "word" ) );
    }

}
