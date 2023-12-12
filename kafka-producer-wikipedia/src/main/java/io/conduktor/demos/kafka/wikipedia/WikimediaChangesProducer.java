package io.conduktor.demos.kafka.wikipedia;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
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

        // cria instancia producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(getProperties());

        String topic = "wikimedia.recent-change";

        EventHandler eventHandler = new WikimediaChangeHandler(producer, topic); // criando o nosso manipulador de eventos
        String urlWikimedia = "https://stream.wikimedia.org/v2/stream/recentchange";

        // cria conexão SSE (Server-Sent Events) tecnologia de comunicação assíncrona entre um servidor e um cliente
        // através de uma única conexão HTTP. Usando o nosso cliente 'eventHandler' e este servidor 'urlWikipedia'
        EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(urlWikimedia));
        EventSource eventSource = builder.build();

        //inicia a conexão SSE após a configuração do objeto EventSource, e começara a ouvir os eventos
        eventSource.start();

        //produzimos por 10 min e bloqueamos o programa até então
        TimeUnit.MINUTES.sleep(10);
    }


    private static Properties getProperties(){
        Properties properties = new Properties();

        // conecta ao kafka local
        properties.setProperty("bootstrap.servers", "172.25.212.73:9092");

        //propriedades do producer
        properties.setProperty("key.serializer", StringSerializer.class.getName()); // Tipo da chave para serializacao
        properties.setProperty("value.serializer", StringSerializer.class.getName()); // Tipo do valor para serializaao
        properties.setProperty("retries", "3"); //Número máximo de tentativas que o produtor fará ao enviar uma mensagem
        properties.setProperty("max.block.ms", "10000"); // Define o tempo máximo que o método send() do produtor aguardará
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true"); // Duplicatas não são introduzidas devido a novas tentativas de rede

        /*os produtores podem optar por receber confirmação de gravações de dados:
        acks = 0 -> O produtor não esperará pela confirmação (possível perda de dados)
        acks = 1 -> O produtor aguardará o reconhecimento do líder (perda limitada de dados)
        acks = all -> Reconhecimento de líder + réplicas (sem perda de dados)*/
        properties.setProperty("acks", "all");

        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20"); //Define o tempo em milissegundos que o produtor aguardará antes de enviar um lote de mensagens.
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024)); //Define o tamanho máximo em bytes de um lote de mensagens a ser enviado pelo produtor.
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy"); // Define o tipo de compressão a ser aplicado as mensagens antes do envio.

        return properties;
    }
}