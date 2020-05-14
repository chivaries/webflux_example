package com.example.webflux;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
public class BookController {

    @GetMapping("/foo")
    public String getFoo() {
        return "foo";
    }

    @GetMapping("/flux")
    public Flux<String> flux() {
        return Flux.just("a","b","c");
    }

    @GetMapping("/mono")
    public Mono<String> mono() {
        return Mono.just("a");
    }
 }
