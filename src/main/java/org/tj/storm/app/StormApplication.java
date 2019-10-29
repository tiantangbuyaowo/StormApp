package org.tj.storm.app;


import io.latent.storm.rabbitmq.RabbitMQSpout;
import io.latent.storm.rabbitmq.config.ConnectionConfig;
import io.latent.storm.rabbitmq.config.ConsumerConfig;
import io.latent.storm.rabbitmq.config.ConsumerConfigBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.hbase.bolt.HBaseBolt;
import org.apache.storm.hbase.bolt.mapper.SimpleHBaseMapper;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.tj.storm.perftab.IndexDataBolt;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

@SpringBootApplication
@Slf4j
public class StormApplication implements CommandLineRunner {

    public static final org.slf4j.Logger logger = LoggerFactory.getLogger( StormApplication.class );

    public static final AtomicInteger count = new AtomicInteger( 0 );

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
       /* hbaseConf.put( "storm.zookeeper.session.timeout", "60000" );
        hbaseConf.put( "storm.zookeeper.retry.times", "9999" );*/
        conf.put( "hbase.conf", hbaseConf );

        TopologyBuilder builder = new TopologyBuilder();

        RabbitMqScheme scheme = new RabbitMqScheme();
        IRichSpout mpSpout = new RabbitMQSpout( scheme );
        ConnectionConfig connectionConfig = new ConnectionConfig( "192.168.3.231", 5672, "admin", "admin", "/", 10 ); // host, port, username, password, virtualHost, heartBeat
        ConsumerConfig spoutConfig = new ConsumerConfigBuilder().connection( connectionConfig )
                .queue( "stormquene" )
                .prefetch( 200 )
                .requeueOnFail()
                .build();
        builder.setSpout( "mpSpout", mpSpout )
                .addConfigurations( spoutConfig.asMap() )
                .setMaxSpoutPending( 200 );


        builder.setBolt( "IndexDataBolt", new IndexDataBolt(), 5 ).shuffleGrouping( "mpSpout" );


        SimpleHBaseMapper mapper = new SimpleHBaseMapper()
                .withRowKeyField( "row" )
                .withColumnFields( new Fields( "entity", "index", "value" ) )
                .withColumnFamily( "p1" );

        HBaseBolt hbaseBolt = new HBaseBolt( "perftab", mapper )
                .withConfigKey( "hbase.conf" ).withBatchSize( 1000 );//如果没有withConfigKey会报错
        builder.setBolt( "HBaseBolt", hbaseBolt ).shuffleGrouping( "IndexDataBolt" );


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
                .withConfigKey( "hbase.conf" ).withBatchSize( 2000 );//如果没有withConfigKey会报错
        builder.setBolt( "HBaseBolt", hbaseBolt ).shuffleGrouping( "CountBolt" );


        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology( "firststorm", conf, builder.createTopology() );
        // Utils.sleep( 30000 );
        // cluster.killTopology( "firststorm" );
        // cluster.shutdown();
    }


}
