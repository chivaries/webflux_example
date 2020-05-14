package com.example.webflux;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.server.RequestPredicate;
import org.springframework.web.reactive.function.server.RequestPredicates;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.ServerResponse;
import reactor.core.publisher.Mono;

import static org.springframework.web.reactive.function.BodyInserters.fromObject;
import static org.springframework.web.reactive.function.server.RequestPredicates.GET;
import static org.springframework.web.reactive.function.server.RequestPredicates.accept;
import static org.springframework.web.reactive.function.server.RouterFunctions.route;


@SpringBootApplication
public class WebfluxApplication {
	@Autowired
	BookHandler bookHandler;

	public static void main(String[] args) {
		SpringApplication.run(WebfluxApplication.class, args);
	}

	@Bean
	public RouterFunction<ServerResponse> helloWorldRoute() {
		return route(RequestPredicates.path("/hello-world"),
				request -> ServerResponse.ok().body(fromObject("Hello World")));
	}

	@Bean
	public RouterFunction<ServerResponse> routeBook() {
		return route(GET("/book").and(accept(MediaType.APPLICATION_JSON)),
				request -> {
					Book book = new Book();
					book.setIsbn("1234");
					book.setTitle("Boot book");

					Mono<Book> mono = Mono.just(book);

					return ServerResponse.ok().contentType(MediaType.APPLICATION_JSON).body(mono, Book.class);
				});
	}

	@Bean
	public RouterFunction<ServerResponse> routeBookComposed() {
		return route(GET("/book").and(accept(MediaType.APPLICATION_JSON)), bookHandler::book)
				.andRoute(GET("/books").and(accept(MediaType.APPLICATION_JSON)), bookHandler::allBooks);
	}
}
