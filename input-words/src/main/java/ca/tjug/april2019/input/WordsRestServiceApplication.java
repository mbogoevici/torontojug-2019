package ca.tjug.april2019.input;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@SpringBootApplication
@RestController
@EnableBinding(Source.class)
public class WordsRestServiceApplication {

	@Autowired
	Source source;

	@PostMapping("/lines")
	public void publishToOutput(@RequestBody String line) {
		source.output().send(MessageBuilder.withPayload(line).build());
	}

	public static void main(String[] args) {
		SpringApplication.run(WordsRestServiceApplication.class, args);
	}
}
