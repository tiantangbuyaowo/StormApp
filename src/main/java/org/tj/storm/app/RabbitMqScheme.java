package org.tj.storm.app;

import org.apache.storm.spout.Scheme;
import org.apache.storm.tuple.Fields;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by tangjing on 2019/10/29.
 */
public class RabbitMqScheme implements Scheme {

    @Override
    public List<Object> deserialize(ByteBuffer byteBuffer) {
        List objs = new ArrayList();
        //直接反序列化为string
        Charset charset = Charset.forName( "utf-8" );
        String str = charset.decode( byteBuffer ).toString();
        //依次返回UUID，String,Number
        //  objs.add(UUID.randomUUID().toString());
        objs.add( str );
        //  String numStr = Math.round(Math.random()*8999+1000)+""; 
        //  objs.add(numStr);
        return objs;
    }


    @Override
    public Fields getOutputFields() {
        return new Fields( "str" );
    }
}
