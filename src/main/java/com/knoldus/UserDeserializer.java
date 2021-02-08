package com.knoldus;

import java.util.Map;
import org.apache.kafka.common.serialization.Deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;

public class UserDeserializer implements Deserializer<UserObject> {
    
    @Override
    public void configure(Map<String, ?> arg0, boolean arg1) {

    }

    @Override
    public UserObject deserialize(String arg0, byte[] arg1) {
        ObjectMapper mapper = new ObjectMapper();
        UserObject user = null;
        try {
            user = mapper.readValue(arg1, UserObject.class);
        } catch (Exception e) {

            e.printStackTrace();
        }
        return user;
    }

    @Override
    public void close() {

    }

}
