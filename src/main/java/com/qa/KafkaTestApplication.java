package com.qa;

import java.util.concurrent.TimeUnit;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

import com.qa.domain.Greeting;
import com.qa.service.MessageListener;
import com.qa.service.MessageProducer;

@SpringBootApplication
public class KafkaTestApplication {

	public static void main(String[] args) throws Exception {

		ConfigurableApplicationContext context = SpringApplication.run(KafkaTestApplication.class, args);

		MessageProducer producer = context.getBean(MessageProducer.class);
		MessageListener listener = context.getBean(MessageListener.class);
		/*
		 * Sending a Hello World message to topic 'baeldung'. Must be recieved by both
		 * listeners with group foo and bar with containerFactory
		 * fooKafkaListenerContainerFactory and barKafkaListenerContainerFactory
		 * respectively. It will also be recieved by the listener with
		 * headersKafkaListenerContainerFactory as container factory
		 */
		producer.sendMessage("Hello, World!");
		listener.getLatch().await(10, TimeUnit.SECONDS);

		/*
		 * Sending message to a topic with 5 partition, each message to a different
		 * partition. But as per listener configuration, only the messages from
		 * partition 0 and 3 will be consumed.
		 */
		for (int i = 0; i < 5; i++) {
			producer.sendMessageToPartion("Hello To Partioned Topic!", i);
		}
		listener.getPartitionLatch().await(10, TimeUnit.SECONDS);

		/*
		 * Sending message to 'filtered' topic. As per listener configuration, all
		 * messages with char sequence 'World' will be discarded.
		 */
		producer.sendMessageToFiltered("Hello Baeldung!");
		producer.sendMessageToFiltered("Hello World!");
		listener.getFilterLatch().await(10, TimeUnit.SECONDS);

		/*
		 * Sending message to 'greeting' topic. This will send and recieved a java
		 * object with the help of greetingKafkaListenerContainerFactory.
		 */
		producer.sendGreetingMessage(new Greeting("Greetings", "World!"));
		listener.getGreetingLatch().await(10, TimeUnit.SECONDS);

		context.close();
	}

}
