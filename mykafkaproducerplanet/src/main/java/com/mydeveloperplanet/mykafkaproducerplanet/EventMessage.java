package com.mydeveloperplanet.mykafkaproducerplanet;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class EventMessage {

    public String key;
    public String eventData;

}
