package org.sunbird.nlw.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.sunbird.common.util.CbExtServerProperties;
import org.sunbird.common.util.Constants;
import org.sunbird.core.producer.Producer;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

@Service
public class ClaimEventKarmaPointsServiceImpl {


    @Autowired
    Producer producer;

    @Autowired
    CbExtServerProperties serverProperties;

    public void generateKarmaPointEventAndPushToKafka(String userId, String eventId, String batchId, Map<String, Object> eventDetails) {

        long ets = ((Date) eventDetails.get(Constants.END_DATE)).getTime() - 10 * 1000;

        Map<String, Object> objectMap = new HashMap<>();
        objectMap.put("user_id", userId);
        objectMap.put("etc", ets);
        objectMap.put("event_id", eventId);
        objectMap.put("batch_id", batchId);

        producer.pushWithKey(serverProperties.getUserEventKarmaPointTopic(), objectMap, userId);

    }
}