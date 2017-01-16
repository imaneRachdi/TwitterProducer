/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

import java.io.Serializable;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.twitter.hbc.core.endpoint.StreamingEndpoint;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import com.twitter.hbc.core.endpoint.Location;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer08;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

public class FlinkTwitterProducer  implements Serializable {


    public static void run(String consumerKey, String consumerSecret,
                           String token, String secret,String topic,String broker_list) throws InterruptedException {

        Properties properties = new Properties();
        properties.put("metadata.broker.list", broker_list);
        properties.put("serializer.class", "kafka.serializer.StringEncoder");
        properties.put("client.id","camus");
        ProducerConfig producerConfig = new ProducerConfig(properties);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties p = new Properties();
     p.setProperty(TwitterSource.CONSUMER_KEY, consumerKey);
        p.setProperty(TwitterSource.CONSUMER_SECRET, consumerSecret);
        p.setProperty(TwitterSource.TOKEN, token);
        p.setProperty(TwitterSource.TOKEN_SECRET, secret);


        TwitterSource.EndpointInitializer endFilt = new Filterimplements(topic) ;

		while(true)		{
            TwitterSource source = new TwitterSource(p);
            source.setCustomEndpointInitializer(endFilt);

            DataStream<String> streamSource = env.addSource(source) ;


				streamSource.addSink(new FlinkKafkaProducer08<String>(broker_list, topic, new SimpleStringSchema()));

env.execute() ; 
		}





    }



}
