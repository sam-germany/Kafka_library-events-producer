package com.learnkafka.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.domain.Book;
import com.learnkafka.domain.LibraryEvent;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.SettableListenableFuture;

import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class LibraryEventProducerUnitTest {

    @Mock
    KafkaTemplate<Integer,String> kafkaTemplate;      //Note: their is no explicit connection given in between "kafkaTemplate" and "libraryEventProducer"
    @Spy                                              // e.g   @Mock and @InjectMocks but internally @Mock in injected inside the @InjectMocks
    ObjectMapper objectMapper = new ObjectMapper();
    @InjectMocks
    LibraryEventProducer libraryEventProducer;

    @Test
    void sendLibraryEvent_Approach2_failure() throws JsonProcessingException, ExecutionException, InterruptedException {
//given
        Book book22 = Book.builder()
                .bookId(123)
                .bookAuthor("Sunny")
                .bookName("Kafka with Spring boot----")
                .build();

        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                .book(book22)
                .build();

        SettableListenableFuture future22 = new SettableListenableFuture();
        future22.setException(new RuntimeException("Exception calling kafka........."));

        when(kafkaTemplate.send(isA(ProducerRecord.class))).thenReturn(future22);

//when
        assertThrows(Exception.class, () -> libraryEventProducer.sendLibraryEvent_Approach2(libraryEvent).get());

        // as above explains that their is internal connection between @Mock and  @InjectMocks so here we
        // are directly calling the
    }


    @Test
    void sendLibraryEvent_Approach2_success() throws JsonProcessingException, ExecutionException, InterruptedException {
//given
        Book book22 = Book.builder()
                .bookId(123)
                .bookAuthor("Sunny")
                .bookName("Kafka with Spring boot----")
                .build();

        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                .book(book22)
                .build();
        String record22 = objectMapper.writeValueAsString(libraryEvent);

        SettableListenableFuture future22 = new SettableListenableFuture();

        ProducerRecord<Integer, String> producerRecord22  = new ProducerRecord("library-events", libraryEvent.getLibraryEventId(), record22);
        RecordMetadata recordMetadata22 =
                      new RecordMetadata(new TopicPartition("library-events",1),1,1,342,
                                                                              System.currentTimeMillis(), 1,2);

        SendResult<Integer,String> sendResult = new SendResult<Integer,String>(producerRecord22, recordMetadata22 );

        future22.set(sendResult);
        when(kafkaTemplate.send(isA(ProducerRecord.class))).thenReturn(future22);

//when
        ListenableFuture<SendResult<Integer,String>> listenableFuture = libraryEventProducer.sendLibraryEvent_Approach2(libraryEvent);

//then
        SendResult<Integer,String> sendResult1 = listenableFuture.get();
        assert sendResult1.getRecordMetadata().partition() == 1;

    }


}
