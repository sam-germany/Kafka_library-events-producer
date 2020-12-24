package com.learnkafka.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.producer.LibraryEventProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.web.servlet.MockMvc;

//import static org.mockito.ArgumentMatchers.eq;
//import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;

@WebMvcTest(LibraryEventsController.class)
@AutoConfigureMockMvc
public class LibraryEventControllerUnitTest {

    @Autowired
    MockMvc mockMvc;

    @MockBean
    LibraryEventProducer libraryEventProducer22;

    ObjectMapper objectMapper22 = new ObjectMapper();
/*
    @Test
    void postLibraryEvent() throws Exception {
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
                         // converting libraryEvent22 object to String
        String json22 = objectMapper22.writeValueAsString(libraryEvent);

          doNothing()                       // he takes this line as sendLibraryEvent_Approach2() method is a asynchronous method and also
          .when(libraryEventProducer22)       // of void type so nothing will be returned from here so he uses  doNothing()
          .sendLibraryEvent_Approach2( isA(LibraryEvent.class) );

//expect
      mockMvc.perform(  post("/v1/libraryevent")       // this line of code means just send this request to LibraryEventsController
                        .content(json22)                         // with this data (json22)  and expect a response as  isCreated()
                        .contentType(MediaType.APPLICATION_JSON)  // if it return isCreated() then test is passed otherwise it fails
                     ).andExpect(status().isCreated());
    }


    @Test
    void postLibraryEvent_4xx_Error() throws Exception {
//given
        Book book22 = Book.builder()
                          .bookId(null)
                          .bookAuthor(null)
                          .bookName("Kafka with Spring boot----")
                          .build();

        LibraryEvent libraryEvent = LibraryEvent.builder()
                                                .libraryEventId(null)
                                                .book(book22)
                                                .build();
        // converting libraryEvent22 object to String
        String json22 = objectMapper22.writeValueAsString(libraryEvent);

        doNothing()                       // he takes this line as sendLibraryEvent_Approach2() method is a asynchronous method and also
                .when(libraryEventProducer22)       // of void type so nothing will be returned from here so he uses  doNothing()
                .sendLibraryEvent_Approach2( isA(LibraryEvent.class) );

//expect
        String expectedErrorMessage22 = "book.bookAuthor - must not be blank, book.bookId - must not be null";

        mockMvc.perform(  post("/v1/libraryevent")  // this line of code means just send this request to LibraryEventsController
                         .content(json22)                               // with this data (json22)  and expect a response as  isCreated()
                         .contentType(MediaType.APPLICATION_JSON)       // if it return isCreated() then test is passed otherwise it fails
              ).andExpect(status().is4xxClientError())
               .andExpect(content().string(expectedErrorMessage22));
    }
*/

}
