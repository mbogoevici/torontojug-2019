package ca.tjug.april2019.wordcount;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.binder.kafka.streams.annotations.KafkaStreamsProcessor;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.messaging.handler.annotation.SendTo;

import java.util.Arrays;
import java.util.regex.Pattern;

@SpringBootApplication
@EnableBinding(KafkaStreamsProcessor.class)
public class WordCountApplication {


    @StreamListener("input")
    @SendTo("output")
    public KStream<String, Long> process(KStream<String, String> textLines) {


        final Pattern pattern = Pattern.compile("\\W+", Pattern.UNICODE_CHARACTER_CLASS);

        final KTable<String, Long> wordCounts = textLines
                // Split each text line, by whitespace, into words.  The text lines are the record
                // values, i.e. we can ignore whatever data is in the record keys and thus invoke
                // `flatMapValues()` instead of the more generic `flatMap()`.
                .flatMapValues(value -> Arrays.asList(pattern.split(value.toLowerCase())))
                // Count the occurrences of each word (record key).
                //
                // This will change the stream type from `KStream<String, String>` to `KTable<String, Long>`
                // (word -> count).  In the `count` operation we must provide a name for the resulting KTable,
                // which will be used to name e.g. its associated state store and changelog topic.
                //
                // Note: no need to specify explicit serdes because the resulting key and value types match our default serde settings
                .groupBy((key, word) -> word)
                .count();

        // Write the `KTable<String, Long>` to the output topic.
//        KStream<String, Long> wordCountsStream = wordCounts.toStream();
//        wordCountsStream.to("streams-wordcount-output", Produced.with(Serdes.String(), Serdes.Long()));

        return wordCounts.toStream().map((s, aLong) -> {
            System.out.println(s + " " + aLong);
            return KeyValue.pair(s, aLong);
        });
    }

    public static void main(String[] args) {
        SpringApplication.run(WordCountApplication.class, args);
    }
}
