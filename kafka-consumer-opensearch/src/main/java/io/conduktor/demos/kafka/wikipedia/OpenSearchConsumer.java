package io.conduktor.demos.kafka.wikipedia;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;


public class OpenSearchConsumer {
    private static final Logger logger = LoggerFactory.getLogger(OpenSearchConsumer.class.getSimpleName());

    public static void main(String[] args) throws IOException {
        logger.info("I am a kafka Consumer!");

        // primeiro cria um cliente opensearch
        RestHighLevelClient openSearchClient = createOpenSearchClient();

        //cria nosso cliente kafka
        KafkaConsumer<String, String> consumer = createKafkaConsumer();

        //precisamos criar o índice no OpenSearch se ele ainda não existir
        try(openSearchClient; consumer){

            boolean indexExists = openSearchClient.indices().exists(new GetIndexRequest("wikimedia"), RequestOptions.DEFAULT);

            if(!indexExists){
                CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikimedia");
                openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                logger.info("O Índice Wikimedia foi criado!");
            }else{
                logger.info("O Índice Wikimedia já existe");
            }

            //nós subscrevemos o consumidor
            consumer.subscribe(Collections.singleton("wikimedia.recent-change"));

            while(true){

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));

                int recordCount = records.count();
                logger.info("Recebido " + recordCount + " record(s)");

                //envio de lote de mensagem
                BulkRequest bulkRequest = new BulkRequest();


                for (ConsumerRecord<String, String> record : records){

                    //extraímos o ID do valor JSON
                    String id = extractId(record.value());
                    try {
                    //envia registro para o OpenSearch
                    IndexRequest indexRequest = new IndexRequest("wikimedia")
                            .source(record.value(), XContentType.JSON)
                            .id(id);

                    IndexResponse indexResponse = openSearchClient.index(indexRequest, RequestOptions.DEFAULT);
                    logger.info("RESPOSTA ID: " + indexResponse.getId());
                    } catch (Exception e){
                        logger.error("Erro ao enviar a mensagem: " + e.getMessage());
                    }

                }

                // Se o lote de mensagem for maior que 0
                if (bulkRequest.numberOfActions() > 0){
                    BulkResponse bulkResponse = openSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                    logger.info("Inserted " + bulkResponse.getItems().length + " record(s).");

                    try {
                        Thread.sleep(10000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                }
            }

        }

    }

    public static RestHighLevelClient createOpenSearchClient() {
        String connString = "http://localhost:9200";

        // construímos um URI a partir da string de conexão
        RestHighLevelClient restHighLevelClient;
        URI connUri = URI.create(connString);

        // extrair informações de login, se existirem
        String userInfo = connUri.getUserInfo();

        if (userInfo == null) {
            // Cliente REST sem segurança
            restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), "http")));

        } else {
            // Cliente REST com segurança
            String[] auth = userInfo.split(":");

            CredentialsProvider cp = new BasicCredentialsProvider();
            cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
                            .setHttpClientConfigCallback(
                                    httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                                            .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));


        }
        return restHighLevelClient;
    }

    private static KafkaConsumer<String, String> createKafkaConsumer(){

        String groupId = "consumer-opensearch-demo";

        // create consumer configs
        Properties properties = new Properties();
        properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "172.25.212.73:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        // create consumer
        return new KafkaConsumer<>(properties);
    }

    private static String extractId(String json){
        // gson library

        try{
        return JsonParser.parseString(json)
                .getAsJsonObject()
                .get("meta")
                .getAsJsonObject()
                .get("id")
                .getAsString();
        }catch (Exception e){
            logger.error("Erro ao formatar a mensagem: " + e.getMessage());
            return "";
        }
    }
}