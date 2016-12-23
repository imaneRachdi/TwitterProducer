/**
 * Created by imen on 23/12/2016.
 */
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

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



public class TwitterProducer {


    public static void main(String[] args) {
        String topic = args[0];


        ParameterTool tool = ParameterTool.fromArgs(args);

        Properties properties = new Properties();
        properties.put("metadata.broker.list", tool.getRequired("broker_list"));
       // properties.put("metadata.broker.list", "localhost:9092");
System.out.print(tool.getRequired("broker_list"));
        System.out.print(tool.getRequired("topic"));
        properties.put(topic, tool.getRequired("topic"));

        properties.put("serializer.class", "kafka.serializer.StringEncoder");
        properties.put("client.id","camus");
        ProducerConfig producerConfig = new ProducerConfig(properties);
        kafka.javaapi.producer.Producer<String, String> producer = new kafka.javaapi.producer.Producer<String, String>(
                producerConfig);


        BlockingQueue<String> queue = new LinkedBlockingQueue<String>(100000);
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
       // endpoint.trackTerms(Lists.newArrayList("Paris","London","New York"));
/*
endpoint.locations(Lists.newArrayList(new Location(new Location.Coordinate(-73.935242,40.730610),new Location.Coordinate(-74.935242,41.730610) )));
//Paris
               endpoint.locations(Lists.newArrayList( new Location(new Location.Coordinate(2.294694,48.858093),new Location.Coordinate(2.19,49) )));
                //london
        endpoint.locations(Lists.newArrayList(  new Location(new Location.Coordinate(-0.076132,51.508530),new Location.Coordinate(0.75,51.30) )));
                       // new Location.Coordinate(2.294694,48.858093),
                        //new Location.Coordinate(	-0.076132,51.508530	) ) ;
                        */
        String consumerKey=   "to15zikqFu3KFvRGr2fYCQ";
        String consumerSecret="pZ64dkSQNqeax5ddkZI8qS4Ut8wIEzyFglMot6YVqw8";
        String accessToken="302091857-c2k1RXdZW5kvwObIHu91rEqlVpKT64GAgvwAJVCJ";
        String accessTokenSecret="eVc8vQ6vgXh7IzWh7W7jjdgTEf9kSTcL4EVVP3qvqck31";

        Authentication auth = new OAuth1(consumerKey, consumerSecret, accessToken,
                accessTokenSecret);
System.out.print("connex");
        Client client = new ClientBuilder().hosts(Constants.STREAM_HOST)
                .endpoint(endpoint).authentication(auth)
                .processor(new StringDelimitedProcessor(queue)).build();
        System.out.print("connex");
        // Establish a connection
        client.connect();
        System.out.print("connex");
        for (int msgRead = 0; msgRead < 1000; msgRead++) {
            KeyedMessage<String, String> message = null;
            try {
                System.out.print("conjkkkkknex");

                message = new KeyedMessage<String, String>(topic, queue.take());
                System.out.print("conjkkkkknex");

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            producer.send(message);
        }
        producer.close();
        client.stop();




    }


}