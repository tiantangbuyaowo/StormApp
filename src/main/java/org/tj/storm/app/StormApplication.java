package org.tj.storm.app;


import lombok.extern.slf4j.Slf4j;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.HashMap;
import java.util.Map;

@SpringBootApplication
@Slf4j
public class StormApplication implements CommandLineRunner {

    public static final org.slf4j.Logger logger = LoggerFactory.getLogger( StormApplication.class );


    public static void main(String[] args) {
        SpringApplication.run( StormApplication.class, args );
    }


    @Override
    public void run(String... args) throws Exception {
        Config conf = new Config();
        /*
        Map<String, Object> hbaseConf = new HashMap<String, Object>();
        hbaseConf.put( "hbase.rootdir", "hdfs://hadoopcluster/hbase" );
        hbaseConf.put( "hbase.zookeeper.quorum", "node1:2181,node2:2181,node3:2181" );
        config.put( "hbase.conf", hbaseConf );*/

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout( "RandomSpout", new RandomSpout() );
        builder.setBolt( "SplitBolt", new SplitBolt() ).shuffleGrouping( "RandomSpout" );
        builder.setBolt( "CountBolt", new CountBolt() ).fieldsGrouping( "SplitBolt", new Fields( "word" ) );


        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology( "firststorm", conf, builder.createTopology() );
       // Utils.sleep( 30000 );
       // cluster.killTopology( "firststorm" );
       // cluster.shutdown();


    }


}
