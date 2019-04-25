package ca.tjug.april2019;

import org.apache.camel.Message;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.model.rest.RestBindingMode;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;

@SpringBootApplication
@RestController
public class DemoWordCountRestApplication {

	private ConcurrentMap<String,Long> wordCount = new ConcurrentHashMap<>();

	private List<WordCount> updates = new CopyOnWriteArrayList<>();

	@Bean
	public RouteBuilder routeBuilder() {
		return new RouteBuilder() {
			@Override
			public void configure() throws Exception {

				routeBuilder()
						.restConfiguration("servlet")
						.bindingMode(RestBindingMode.auto);

				from("kafka:word-count?valueDeserializer=org.apache.kafka.common.serialization.LongDeserializer&seekTo=beginning")
						.log("${in.headers[kafka.KEY]} : ${in.body}")
						.process(exchange -> {
							Message in = exchange.getIn();
							wordCount.put(in.getHeader(KafkaConstants.KEY, String.class), in.getBody(Long.class));
							updates.add(new WordCount(in.getHeader(KafkaConstants.KEY, String.class), in.getBody(Long.class)));
						});

				rest("/word-count")
						.get().route().setBody(constant(wordCount)).endRest();

				rest("/updates")
						.get().route().setBody(constant(updates)).endRest();

			}
		};
	}

	public static void main(String[] args) {
		SpringApplication.run(DemoWordCountRestApplication.class, args);
	}
}
