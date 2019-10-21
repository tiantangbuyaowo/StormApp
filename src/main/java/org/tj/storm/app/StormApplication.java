package org.tj.storm.app;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@Slf4j
public class StormApplication implements CommandLineRunner {

    public static final org.slf4j.Logger logger = LoggerFactory.getLogger( StormApplication.class );


    public static void main(String[] args) {
        SpringApplication.run( StormApplication.class, args );
    }


    @Override
    public void run(String... args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout( "spout", new RandomSpout() );
        builder.setBolt( "bolt", new SenqueceBolt() ).shuffleGrouping( "spout" );
        Config conf = new Config();
        conf.setDebug( false );
       /* if (args != null && args.length > 0) {
            conf.setNumWorkers( 3 );
            try {
                StormSubmitter.submitTopology( args[0], conf, builder.createTopology() );
            } catch (AlreadyAliveException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (InvalidTopologyException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        } else {*/
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology( "firststorm", conf, builder.createTopology() );
            Utils.sleep( 30000 );
            cluster.killTopology( "firststorm" );
            cluster.shutdown();
       /* }*/

    }


}
