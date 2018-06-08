package org.wuruihe;

import dao.HbaseDao;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import utils.PropertiesUtils;

import java.io.IOException;
import java.text.ParseException;
import java.util.Collections;

public class KafkaConsumers {
    public static void main(String[] args) throws IOException {
        KafkaConsumer<String,String> kafkaConsumer=new KafkaConsumer<String, String>(PropertiesUtils.properties);
        kafkaConsumer.subscribe(Collections.singletonList(PropertiesUtils.properties.getProperty("kafka.topics")));
        //创建HbaseDao
        HbaseDao hbaseDao=new HbaseDao();
        while(true){
            ConsumerRecords<String, String> records=kafkaConsumer.poll(300);
            for (ConsumerRecord<String, String> record : records) {
                String ori = record.value();
                System.out.println(ori);
                try {
                    hbaseDao.put(ori);
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            }

        }
    }
}
