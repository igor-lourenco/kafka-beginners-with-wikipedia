package io.conduktor.demos.kafka.wikipedia;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikimediaChangeHandler implements EventHandler { //classe onde vai ser implementado o manipulador de eventos
    private static final Logger logger = LoggerFactory.getLogger(WikimediaChangeHandler.class.getSimpleName());
    KafkaProducer<String, String> kafkaProducer;
    String topic;

    public WikimediaChangeHandler(KafkaProducer<String, String> kafkaProducer, String topic) {
        this.kafkaProducer = kafkaProducer;
        this.topic = topic;
    }

    @Override // Quando o fluxo estiver aberto
    public void onOpen() throws Exception {
        logger.info("Conexão SSE aberta com o https://stream.wikimedia.org/v2/stream/recentchange");
    }

    @Override // Quando estamos fechando o fluxo de leitura
    public void onClosed() throws Exception {
        logger.info("Fechando a conexão com o Kafka");
        kafkaProducer.close(); // fecha a conexão com o kafka
        logger.info("Fechando a conexão com https://stream.wikimedia.org/v2/stream/recentchange");
    }

    @Override // Quando o fluxo recebe a mensagem
    public void onMessage(String s, MessageEvent messageEvent) throws Exception {

        logger.info("Tipo de evento :: {}", s);
        logger.info("Recebendo {} :: {}", s, messageEvent.getData());

        //assincrono
        kafkaProducer.send(new ProducerRecord<>(topic, messageEvent.getData())); // envia a mensagem recebida pro topic do kafka especificado
    }

    @Override // Chamado quando o servidor envia um comentário durante a transmissão de eventos SSE
    public void onComment(String s) throws Exception {
        logger.info("Comentario: {}", s);
    }

    @Override // Quando ocorrer algum erro de conexão
    public void onError(Throwable throwable) {
        logger.error("Erro ao ler o Stream :: {}", throwable.getMessage());

    }


}
