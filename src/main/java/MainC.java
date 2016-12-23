/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
To see The tweets you must  onsume messages on topic twitter-topic to verify the incoming message stream.
Shell
bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic <topic-name> --from-beginning 
but first make sure the zookeeper and the server Kafka are running
/////////
you can provide the topic name as an argument java */
public class MainC {
    public static void main(String[] args) {
		try {
			TwitterKafkaproducer.run("to15zikqFu3KFvRGr2fYCQ", "pZ64dkSQNqeax5ddkZI8qS4Ut8wIEzyFglMot6YVqw8", "302091857-c2k1RXdZW5kvwObIHu91rEqlVpKT64GAgvwAJVCJ","eVc8vQ6vgXh7IzWh7W7jjdgTEf9kSTcL4EVVP3qvqck31",args[0]);
		} catch (InterruptedException e) {
			System.out.println(e);
		}
	}
}
