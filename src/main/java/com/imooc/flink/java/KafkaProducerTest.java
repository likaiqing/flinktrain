package com.imooc.flink.java;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

public class KafkaProducerTest {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer<>(props);

        int i = 0;
        Random random = new Random();

        while (true) {
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date date = new Date();
            String dateStr = format.format(date);
            int r = random.nextInt(100);
//            if (r < 20) {
//                long l = date.getTime() - 10000;
//                dateStr = format.format(new Date(l));
//                System.out.println("延迟5秒发送," + dateStr);
//            }
//            if (r < 10) {
//                long l = date.getTime() - 40000;
//                dateStr = format.format(new Date(l));
//                System.out.println("-----延迟40秒发送," + dateStr);
//            }
            String value = dateStr + "\t" + r;
            producer.send(new ProducerRecord<String, String>("btx_utxo_test2", null, System.currentTimeMillis(), Integer.toString(i), value));
            System.out.println("key:" + i + ";value:" + value);
            i++;
            try {
                Thread.sleep(r * 10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
