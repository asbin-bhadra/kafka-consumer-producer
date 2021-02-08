package com.knoldus;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class Consumer {
    
    public static void main(String[] args) {
       ConsumerListener c = new ConsumerListener();
        Thread thread = new Thread(c);
        thread.start();
    }
    public static void writeToFile(UserObject obj) {  
        try {
            BufferedWriter myWriter = new BufferedWriter(new FileWriter("Users.txt", true));
            myWriter.write(obj.toString());
            myWriter.write('\n');
            myWriter.close();
        } catch (IOException e) {
          System.out.println("An error occurred.");
          e.printStackTrace();
        } 
      } 
      public static void consumer() throws IOException {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("max.block.ms",1000);
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "com.knoldus.UserDeserializer");
        properties.put("group.id", "test-group");

        KafkaConsumer<String, UserObject> consumer = new KafkaConsumer(properties);
        List topics = new ArrayList();
        topics.add("user");
        consumer.subscribe(topics);

        try{
            while (true){
                ConsumerRecords<String, UserObject> records = consumer.poll(1000);
                for (ConsumerRecord<String, UserObject> record: records) {
                    System.out.println(record.value());
                    // writing message to text file
                    writeToFile(record.value());
                }
            }
        }catch (Exception e){
            System.out.println(e.getMessage());
        }finally {
            consumer.close();
        }
    }
}

class ConsumerListener implements Runnable {


    @Override
    public void run() {
        try {
            Consumer.consumer();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
