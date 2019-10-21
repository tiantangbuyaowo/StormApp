package org.tj.storm.app;


import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

/**
 * Created by tangjing on 2019/10/21.
 */
public class SenqueceBolt extends BaseBasicBolt {

    public void execute(Tuple arg0, BasicOutputCollector arg1) {
        String word = (String) arg0.getValue( 0 );
        String out = "Hello " + word + "!";
        System.out.println( out );
    }

    public void declareOutputFields(OutputFieldsDeclarer arg0) {

    }
}
