package io.conduktor.demos.kafka.wikipedia;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;


public class WikimediaChangesProducer {
    private static final Logger logger = LoggerFactory.getLogger(WikimediaChangesProducer.class.getSimpleName());

    public static void main(String[] args) throws InterruptedException {
        logger.info("I am a kafka Producer!");

        Properties properties = new Properties();

        // conecta ao kafka local
        properties.setProperty("bootstrap.servers", "172.25.212.73:9092");

        //propriedades do producer
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        properties.setProperty("acks", "all");
        properties.setProperty("retries", "3"); // número máximo de tentativas de envio
        properties.setProperty("max.block.ms", "10000"); // tempo máximo que o produtor aguardará para se conectar ao broker


        // cria instancia producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        String topic = "wikimedia.recent-change";

        EventHandler eventHandler = new WikimediaChangeHandler(producer, topic); // criando o nosso manipulador de eventos
        String urlWikimedia = "https://stream.wikimedia.org/v2/stream/recentchange";

        // cria uma instancia de conexão SSE (Server-Sent Events) é uma tecnologia de comunicação assíncrona entre um servidor e um cliente através de uma única conexão HTTP.
        // usando o nosso cliente 'eventHandler' e este servidor 'urlWikipedia'
        EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(urlWikimedia));
        EventSource eventSource = builder.build();

        //inicia a conexão SSE após a configuração do objeto EventSource, e começara a ouvir os eventos
        eventSource.start();

        //produzimos por 10 min e bloqueamos o programa até então
        TimeUnit.MINUTES.sleep(10);
    }
}