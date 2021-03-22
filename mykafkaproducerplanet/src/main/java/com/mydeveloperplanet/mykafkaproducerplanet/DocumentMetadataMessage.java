package com.mydeveloperplanet.mykafkaproducerplanet;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class DocumentMetadataMessage {

    public String key;
    public String metaData;

}
