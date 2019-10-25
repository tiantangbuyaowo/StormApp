package org.tj.storm.perftab;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

/**
 * Created by tangjing on 2019/10/24.
 */
public class IndexDataBolt extends BaseBasicBolt {


    /**
     * 业务逻辑：
     * 1）获取每个单词
     * 2）对所有单词进行汇总
     * 3）输出
     */
    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String line = input.getString( 0 );
        System.out.println( line );


    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare( new Fields( "key", "value", "time" ) );
    }

}
