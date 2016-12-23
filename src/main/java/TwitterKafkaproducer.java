/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

import java.util.Properties;
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

public class TwitterKafkaproducer {


	public static void run(String consumerKey, String consumerSecret,
			String token, String secret,String topic,String broker_list) throws InterruptedException {

		Properties properties = new Properties();
		properties.put("metadata.broker.list", broker_list);
		properties.put("serializer.class", "kafka.serializer.StringEncoder");
		properties.put("client.id","camus");
		ProducerConfig producerConfig = new ProducerConfig(properties);
		kafka.javaapi.producer.Producer<String, String> producer = new kafka.javaapi.producer.Producer<String, String>(
				producerConfig);

		BlockingQueue<String> queue = new LinkedBlockingQueue<String>(10000);
		StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
		// add some track terms
		// endpoint.trackTerms(Lists.newArrayList("Paris","London","New York"));

endpoint.locations(Lists.newArrayList(new Location(new Location.Coordinate(-73.935242,40.730610),new Location.Coordinate(-74.935242,41.730610) )));
//Paris
               endpoint.locations(Lists.newArrayList( new Location(new Location.Coordinate(2.294694,48.858093),new Location.Coordinate(2.19,49) )));
                //london
        endpoint.locations(Lists.newArrayList(  new Location(new Location.Coordinate(-0.076132,51.508530),new Location.Coordinate(0.75,51.30) )));
                       // new Location.Coordinate(2.294694,48.858093),
                        //new Location.Coordinate(	-0.076132,51.508530	) ) ;
		Authentication auth = new OAuth1(consumerKey, consumerSecret, token,
				secret);
		// Authentication auth = new BasicAuth(username, password);

		// Create a new BasicClient. By default gzip is enabled.
		Client client = new ClientBuilder().hosts(Constants.STREAM_HOST)
				.endpoint(endpoint).authentication(auth)
				.processor(new StringDelimitedProcessor(queue)).build();

		// Establish a connection
		client.connect();

		// Do whatever needs to be done with messages
while(true)		{	KeyedMessage<String, String> message = null;
			try {
				message = new KeyedMessage<String, String>(topic, queue.take());
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			producer.send(message);
		}

	}



	
}