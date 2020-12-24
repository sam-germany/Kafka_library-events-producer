package com.learnkafka.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.domain.LibraryEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;


@Component
@Slf4j
public class LibraryEventProducer {

    String topic22 = "library-events";

    @Autowired
    KafkaTemplate<Integer,String> kafkaTemplate22;

    @Autowired
    ObjectMapper objectMapper;

    public void sendLibraryEvent22(LibraryEvent libraryEvent) throws JsonProcessingException {

        Integer key = libraryEvent.getLibraryEventId();
        String value = objectMapper.writeValueAsString(libraryEvent);

        ListenableFuture<SendResult<Integer,String>> listenableFuture =  kafkaTemplate22.sendDefault(key,value); // <-- see video 31
                                                                              // when we do not use .get() then it become a asynchronous call so the call come from
        listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {    // from the controller and the call moves with 2 threads
            @Override
            public void onFailure(Throwable ex) {
                handleFailure22(key, value, ex);
            }

            @Override
            public void onSuccess(SendResult<Integer, String> result) {
                handleSuccess22(key, value, result);
            }
        });
    }

    public ListenableFuture<SendResult<Integer,String>> sendLibraryEvent_Approach2(LibraryEvent libraryEvent) throws JsonProcessingException {

        Integer key = libraryEvent.getLibraryEventId();
        String value = objectMapper.writeValueAsString(libraryEvent);

        ProducerRecord<Integer, String>  producerRecord22 = buildProducerRecord22(key, value, topic22);
        ListenableFuture<SendResult<Integer,String>> listenableFuture =  kafkaTemplate22.send(producerRecord22); // <-- see video 31
                                                                      // when we do not use .get() then it become a asynchronous call so the call come from
        listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {    // from the controller and the call moves with 2 threads
            @Override
            public void onFailure(Throwable ex) {
                handleFailure22(key, value, ex);
            }
            @Override
            public void onSuccess(SendResult<Integer, String> result) {
                handleSuccess22(key, value, result);
            }
        });

        return listenableFuture;
    }

    private ProducerRecord<Integer, String> buildProducerRecord22(Integer key, String value, String topic22) {

        List<Header> recordHeaders22 = List.of(new RecordHeader("event-source", "scanner".getBytes()));
            return new ProducerRecord<>(topic22, null, key, value, recordHeaders22);
    }





    public SendResult<Integer,String> sendLibraryEventSynchronous22(LibraryEvent libraryEvent) throws JsonProcessingException, ExecutionException, InterruptedException {

        Integer key = libraryEvent.getLibraryEventId();
        String value = objectMapper.writeValueAsString(libraryEvent);
        SendResult<Integer,String> sendResult22 = null;

        try {
           sendResult22 =  kafkaTemplate22.sendDefault(key, value).get(1, TimeUnit.SECONDS);   // when we use .get() then it become a synchronous call so
        } catch (ExecutionException | InterruptedException e) {              //  the call come from controller will wait for the result, till the given time
            log.error("ExecutionException/InterruptedException Sending the Message and the exception is {}", e.getMessage());
            throw e;
        } catch (Exception e) {
            log.error("Exception Sending the Message and the exception is {}", e.getMessage());
        }
        return sendResult22;
    }

    private void handleFailure22(Integer key, String value, Throwable ex) {
        log.error("Error Sending the Message and the exception is {}", ex.getMessage());
        try {
            throw ex;
        } catch (Throwable throwable) {
            log.error("Error in OnFailure: {}", throwable.getMessage());
        }


    }

    private void handleSuccess22(Integer key, String value, SendResult<Integer, String> result) {
        log.info("Message Sent SuccessFully for the key : {} and the value is {} , partition is {}", key, value, result.getRecordMetadata().partition());
    }





}
