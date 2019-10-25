package org.tj.storm.app;


import lombok.extern.slf4j.Slf4j;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.hbase.bolt.HBaseBolt;
import org.apache.storm.hbase.bolt.mapper.SimpleHBaseMapper;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.tj.storm.perftab.IndexDataBolt;

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
        System.setProperty( "hadoop.home.dir", "E:\\hadoop-2.5.2" );
        perftab();
    }

    /**
     * 操作perftab数据
     */
    public void perftab() throws Exception {

        Config conf = new Config();
        conf.setDebug( true );
        Map<String, Object> hbaseConf = new HashMap<String, Object>();
        hbaseConf.put( "hbase.zookeeper.quorum", "192.168.30.128:2181" );
        conf.put( "hbase.conf", hbaseConf );

        TopologyBuilder builder = new TopologyBuilder();

        KafkaSpoutConfig.Builder<String, String> kafkaBuilder = KafkaSpoutConfig.builder( "node-1:9092,node-2:9092,node-3:9092", "testcheng" );
        //设置kafka属于哪个组
        kafkaBuilder.setGroupId( "testgroup" );
        //创建kafkaspoutConfig
        KafkaSpoutConfig<String, String> build = kafkaBuilder.build();
        //通过kafkaspoutConfig获得kafkaspout
        KafkaSpout<String, String> kafkaSpout = new KafkaSpout<String, String>( build );

        //设置5个线程接收数据
        builder.setSpout( "kafkaSpout", kafkaSpout, 5 );

        builder.setBolt( "IndexDataBolt", new IndexDataBolt(), 2 ).shuffleGrouping( "kafkaSpout" );

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology( "firststorm", conf, builder.createTopology() );
    }

    public void wordCount() throws Exception {
        Config conf = new Config();
        conf.setDebug( true );
        Map<String, Object> hbaseConf = new HashMap<String, Object>();
        //hbaseConf.put( "hbase.rootdir", "hdfs:///hbase-data" );
        hbaseConf.put( "hbase.zookeeper.quorum", "192.168.30.128:2181" );
        conf.put( "hbase.conf", hbaseConf );

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout( "RandomSpout", new RandomSpout() );
        builder.setBolt( "SplitBolt", new SplitBolt() ).shuffleGrouping( "RandomSpout" );
        builder.setBolt( "CountBolt", new CountBolt() ).fieldsGrouping( "SplitBolt", new Fields( "word" ) );


        SimpleHBaseMapper mapper = new SimpleHBaseMapper()
                .withRowKeyField( "word" )
                .withColumnFields( new Fields( "word" ) )
                .withColumnFields( new Fields( "count" ) )
                .withColumnFamily( "cf" );

        HBaseBolt hbaseBolt = new HBaseBolt( "wc", mapper )
                .withConfigKey( "hbase.conf" ).withBatchSize( 1 );//如果没有withConfigKey会报错
        builder.setBolt( "HBaseBolt", hbaseBolt ).shuffleGrouping( "CountBolt" );


        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology( "firststorm", conf, builder.createTopology() );
        // Utils.sleep( 30000 );
        // cluster.killTopology( "firststorm" );
        // cluster.shutdown();
    }


}
