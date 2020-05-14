package com.example.webflux;

import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Component
public class BookHandler {

    public Mono<ServerResponse> book(ServerRequest request) {
        Book book = new Book();
        book.setIsbn("123");
        book.setTitle("Boot book");

        Mono<Book> mono = Mono.just(book);

        return ServerResponse.ok().contentType(MediaType.APPLICATION_JSON).body(mono, Book.class);
    }

    public Mono<ServerResponse> allBooks(ServerRequest request) {
        Book book1 = new Book();
        book1.setIsbn("123");
        book1.setTitle("Boot book");

        Book book2 = new Book();
        book2.setIsbn("123");
        book2.setTitle("Boot book");

        Flux<Book> flux = Flux.just(book1, book2);

        return ServerResponse.ok().contentType(MediaType.APPLICATION_JSON).body(flux, Book.class);
    }
}
