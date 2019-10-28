package org.tj.storm.perftab;


import org.apache.commons.lang3.StringUtils;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.zip.Inflater;


/**
 * Created by tangjing on 2019/10/24.
 */
public class IndexDataBolt extends BaseBasicBolt {

    public static final Integer[] indexs = new Integer[]{100001001, 211002100, 211002202, 211002201, 211002200, 211002302, 211002301, 211002300};

    /**
     * 业务逻辑：
     * 1）获取每个单词
     * 2）对所有单词进行汇总
     * 3）输出
     */
    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        //获取kafka中的值
        String line = input.getString( 4 );
        if (StringUtils.isEmpty( line )) {
            return;
        }
        //line = new String( decompress( line.getBytes() ) );
        List<String> values = Arrays.asList( line.split( "@" ) );
        //先@拆分，然后,拆分
        // System.out.println( line );
        values.forEach( value -> {
            //长度为3，0 entity 1 time 2 index
            String[] datas = value.split( "," );
            String entity = datas[0];
            String time = datas[1];
            if (datas.length != 10) {
                System.out.println( datas );
            }
            for (int i = 2; i < datas.length - 1; i++) {
                collector.emit( new Values( time + "_" + entity + "_" + indexs[i - 2], entity, indexs[i - 2].toString(), datas[i] ) );
            }

        } );


    }

    /**
     * 解压缩
     *
     * @param data 待压缩的数据
     * @return byte[] 解压缩后的数据
     */
    public static byte[] decompress(byte[] data) {
        byte[] output = new byte[0];

        Inflater decompresser = new Inflater();
        decompresser.reset();
        decompresser.setInput( data );

        ByteArrayOutputStream o = new ByteArrayOutputStream( data.length );
        try {
            byte[] buf = new byte[1024];
            while (!decompresser.finished()) {
                int i = decompresser.inflate( buf );
                o.write( buf, 0, i );
            }
            output = o.toByteArray();
        } catch (Exception e) {
            output = data;
            e.printStackTrace();
        } finally {
            try {
                o.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        decompresser.end();
        return output;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare( new Fields( "row", "entity", "index", "value" ) );
    }

}
