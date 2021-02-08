package com.knoldus;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

public class Producer {
    public static void main(String[] args){
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("max.block.ms",1000);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "com.knoldus.UserSerializer");
        
        KafkaProducer kafkaProducer = new KafkaProducer<String, UserObject>(properties);

        List<UserObject> users = new ArrayList<>();

        // Random function ued to generate Age
        Random rd = new Random();

        // Generate a list of 10 user
        for(int i=0;i<10;i++){
            users.add(new UserObject(10+i, "User-"+i+1, rd.nextInt(100), "B.Tech"));
        }

        try{
            for(UserObject user : users){
                System.out.println(user);
                kafkaProducer.send(new ProducerRecord<String, UserObject>("user",user.toString(),user ));
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            kafkaProducer.close();
        }
    }
}
