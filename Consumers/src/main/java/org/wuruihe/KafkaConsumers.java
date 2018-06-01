package org.wuruihe;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import utils.PropertiesUtils;

import java.util.Collections;

public class KafkaConsumers {
    public static void main(String[] args) {
        KafkaConsumer<String,String> kafkaConsumer=new KafkaConsumer<String, String>(PropertiesUtils.properties);
        kafkaConsumer.subscribe(Collections.singletonList(PropertiesUtils.properties.getProperty("kafka.topics")));
        while(true){
            ConsumerRecords<String, String> records=kafkaConsumer.poll(300);
            for (ConsumerRecord<String, String> record : records) {
                String ori = record.value();
                System.out.println(ori);
            }

        }
    }
}
