package service;

import deserializable.SalesDeserializable;
import model.Venda;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.Random;

public class SalesProcessor {

    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, SalesDeserializable.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try( KafkaConsumer<String, Venda>  kafkaConsumer  = new KafkaConsumer<String, Venda>(properties)){

            kafkaConsumer.subscribe(Arrays.asList("venda-ingressos"));

            while(true){

                ConsumerRecords<String, Venda> vends = kafkaConsumer.poll(Duration.ofMillis(200));
                for (ConsumerRecord<String, Venda> record : vends) {

                    Venda venda = record.value();

                    if(new Random().nextBoolean()){
                        venda.setStatus("APROVADA");
                    }else{
                        venda.setStatus("REPROVADA");
                    }
                    Thread.sleep(500);
                    System.out.println(venda);
                }
            }
        }catch(Exception ex){
            System.out.println(ex.getMessage());
        }
    }
}
