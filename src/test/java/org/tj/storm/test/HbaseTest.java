package org.tj.storm.test;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.junit.Test;
import org.tj.storm.app.HBaseService;

import java.util.Map;

public class HbaseTest {


    @Test
    public void deleteTable() {
        System.setProperty( "hadoop.home.dir", "E:\\hadoop-2.5.2" );
        org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
        conf.set( "hbase.zookeeper.quorum", "192.168.30.128:2181" );
        conf.set( "hbase.client.keyvalue.maxsize", "500000" );

        HBaseService hbaseService = new HBaseService( conf );
        //删除表
        hbaseService.deleteTable( "wc" );

    }

    @Test
    public void searchData() {
        System.setProperty( "hadoop.home.dir", "E:\\hadoop-2.5.2" );
        org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
        conf.set( "hbase.zookeeper.quorum", "192.168.30.128:2181" );
        conf.set( "hbase.client.keyvalue.maxsize", "500000" );

        HBaseService hbaseService = new HBaseService( conf );
        //删除表
        Map<String, Map<String, String>> data = hbaseService.getResultScannerPrefixFilter( "wc", "w" );
        data.forEach( (k, value) -> {
            System.out.println( k );
        } );


    }

    @Test
    public void getRowData() {
        System.setProperty( "hadoop.home.dir", "E:\\hadoop-2.5.2" );
        org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
        conf.set( "hbase.zookeeper.quorum", "192.168.30.128:2181" );
        conf.set( "hbase.client.keyvalue.maxsize", "500000" );

        HBaseService hbaseService = new HBaseService( conf );
        //删除表
        //hbaseService.deleteTable( "wc" );
        //创建表
        //hbaseService.createTableBySplitKeys("test_base", Arrays.asList("f","back"),hbaseService.getSplitKeys(null));
        //System.out.println( hbaseService.getColumnValue( "wc", " apple", "cf", "count" ) );
        //查询数据
        //1. 根据rowKey查询
        System.out.println( "+++++++++++根据rowKey查询开始+++++++++++" );
        Map<String, String> result1 = hbaseService.getRowData( "perftab", "142860_211002302_/dev_20191029140647" );
        System.out.println( "+++++++++++根据rowKey查询结束+++++++++++" );
        result1.forEach( (k, value) -> {
            System.out.println( k + "-----" + value );
        } );
        System.out.println();
    }

    @Test
    public void getResultScannerPrefixFilter() {
        System.setProperty( "hadoop.home.dir", "E:\\hadoop-2.5.2" );
        org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
        conf.set( "hbase.zookeeper.quorum", "192.168.30.128:2181" );
        conf.set( "hbase.client.keyvalue.maxsize", "500000" );

        HBaseService hbaseService = new HBaseService( conf );
        //删除表
        //hbaseService.deleteTable( "wc" );
        //创建表
        //hbaseService.createTableBySplitKeys("test_base", Arrays.asList("f","back"),hbaseService.getSplitKeys(null));
        //System.out.println( hbaseService.getColumnValue( "wc", " apple", "cf", "count" ) );
        //查询数据
        //1. 根据rowKey查询
        Map<String, Map<String, String>> result1 = hbaseService.getResultScannerPrefixFilter( "perftab", "142860_211002302_/dev_2019102914" );
        System.out.println( "+++++++++++根据rowKey查询+++++++++++" );
        result1.forEach( (k, value) -> {
            System.out.println( k + "-----" + value );
        } );
        System.out.println();
    }

    @Test
    public void getResultScanner() {
        System.setProperty( "hadoop.home.dir", "E:\\hadoop-2.5.2" );
        org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
        conf.set( "hbase.zookeeper.quorum", "192.168.30.128:2181" );
        conf.set( "hbase.client.keyvalue.maxsize", "500000" );

        HBaseService hbaseService = new HBaseService( conf );
        //删除表
        //hbaseService.deleteTable( "wc" );
        //创建表
        //hbaseService.createTableBySplitKeys("test_base", Arrays.asList("f","back"),hbaseService.getSplitKeys(null));
        //System.out.println( hbaseService.getColumnValue( "wc", " apple", "cf", "count" ) );
        //查询数据
        //1. 根据rowKey查询
        long start = System.currentTimeMillis();
        Map<String, Map<String, String>> result1 = hbaseService.getResultScanner( "perftab", "207831_211002300_-1_20191030000000", "207831_211002300_-1_20191031000000" );
        System.out.println(System.currentTimeMillis() - start);
        System.out.println( "+++++++++++根据rowKey查询+++++++++++" );
        result1.forEach( (k, value) -> {
            System.out.println( k + "-----" + value );
        } );
        System.out.println();
    }


}
