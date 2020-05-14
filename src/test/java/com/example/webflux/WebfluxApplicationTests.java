package com.example.webflux;

import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

//@RunWith(SpringRunner.class)
//@SpringBootTest
@Slf4j
public class WebfluxApplicationTests {

	@Test
	public void contextLoads() {
	}

	@Test
	public void generateTest() {
		Flux<String> flux = Flux.generate(
			() -> { // Callable<S> stateSupplier
				return 0;
			},
			(state, sink) -> { // BiFunction<S, SynchronousSink<T>, S> generator
				sink.next("3 x " + state + " = " + 3 * state);
				if (state == 10) {
					sink.complete();
				}
				return state + 1;
			});

		flux.subscribe(System.out::println);
	}

	@Test
	public void fluxTestSyncBlocking() {
		final Flux<List<String>> basketFlux = getFruitsFlux();

		// 절차 지향으로 생각하면 하나의 for each loop 안에서 2가지를 한 번에 해결할 수 있는데
		// 여기서는 총 2번 basket을 순회하고, 특별히 스레드를 지정하지 않았기 때문에 동기, 블록킹 방식으로 동작
		basketFlux.concatMap(basket -> {
			final Mono<List<String>> distinctFruits = Flux.fromIterable(basket).distinct().collectList();
			final Mono<Map<String, Long>> countFruitsMono = Flux.fromIterable(basket)
					.groupBy(fruit -> fruit)
					.concatMap(groupedFlux -> groupedFlux.count()
							.map(count -> {
								final Map<String, Long> fruitCount = new LinkedHashMap<>();
								fruitCount.put(groupedFlux.key(), count);
								return fruitCount;
							})
					)
					.reduce((accumulatedMap, currentMap) -> new LinkedHashMap<String, Long>() { {
						putAll(accumulatedMap);
						putAll(currentMap);
					}});
			return Flux.zip(distinctFruits, countFruitsMono, (distinct, count) -> new FruitInfo(distinct, count));
		}).subscribe(s -> log.info(s.toString()));
	}

	@Test
	public void fluxTestAsyncNonBlocking() throws InterruptedException {
		final Flux<List<String>> basketFlux = getFruitsFlux();

		CountDownLatch countDownLatch = new CountDownLatch(1);

		// 절차 지향으로 생각하면 하나의 for each loop 안에서 2가지를 한 번에 해결할 수 있는데 여기서는 총 2번 basket을 순회
		basketFlux.concatMap(basket -> {
			final Mono<List<String>> distinctFruits = Flux.fromIterable(basket).log().distinct().collectList().log().subscribeOn(Schedulers.parallel());;
			final Mono<Map<String, Long>> countFruitsMono = Flux.fromIterable(basket).log()
					.groupBy(fruit -> fruit)
					.concatMap(groupedFlux -> groupedFlux.count()
							.map(count -> {
								final Map<String, Long> fruitCount = new LinkedHashMap<>();
								fruitCount.put(groupedFlux.key(), count);
								return fruitCount;
							})
					)
					.reduce((accumulatedMap, currentMap) -> new LinkedHashMap<String, Long>() { {
						putAll(accumulatedMap);
						putAll(currentMap);
					}})
					// 비동기 병렬로 subscribe
					.subscribeOn(Schedulers.parallel());

			return Flux.zip(distinctFruits, countFruitsMono, (distinct, count) -> new FruitInfo(distinct, count));
		}).subscribe(s -> log.info(s.toString()), // 값이 넘어올 때 호출, onNext(T)
				error -> {
					log.error(error.getMessage());
					countDownLatch.countDown();
				}, // 에러 발생시 출력하고 countDown, onError(Throwable)
				() -> {
					log.info("complete");
					countDownLatch.countDown();
				} // 정상적 종료시 countDown, onComplete()
		);

		countDownLatch.await();
	}

	/*
		Connectable Flux는 Cold에서 Hot으로 바꾸기 위해서는 Connectable Flux로 변환하는 과정이 필요합니다.
		공식문서에 설명되어 있듯 기본적으로 publish라는 연산자를 호출하면 바꿀 수 있습니다.
		이렇게 변환된 Flux에서 connect()라는 메서드를 호출할 수 있는데,
		이 메서드가 여러 구독자들이 Connectable Flux를 구독한 후 값을 생성하여 각 구독자에게 보내기 시작하게 하는 메서드입니다.

		즉, 우리의 예제에서는 distinctFruits와 countFruitsMono가 구독을 모두 완료한 후에 connect()를 호출할 수 있게 해주면 됩니다.

		어떻게 할 수 있을까요? 다행히 Reactor에서는 autoConnect나 refCount에 인자 값으로 최소 구독하는 구독자의 개수를 지정해서
		이 개수가 충족되면 자동으로 값을 생성할 수 있게 연산자를 제공합니다.

		2개의 차이점이 있다면 autoConnect는 이름 그대로 최소 구독 개수를 만족하면 자동으로 connect()를 호출하는 역할만 하고,
		refCount는 autoConnect가 하는 일에 더해서 구독하고 있는 구독자의 개수를 세다가 하나도 구독하는 곳이 없으면 기존 소스의 스트림도 구독을 해제하는 역할을 합니다.

		interval처럼 무한히 일정 간격으로 값이 나오는 스트림을 Hot으로 바꾼다면 refCount를 고려해 볼 수 있을 것입니다.
		호출되는 소스가 무한으로 값을 생성한다면 더 이상 구독을 안 할 때 해제하게 refCount를 고려해볼 수 있지만,
		우리는 제한된 개수의 데이터만 다 다루면 알아서 완료가 되기 때문에 autoConnect로 충분하다고 생각합니다.

		참고로 여기서 소개한 2개의 연산자는 인자 값을 없을 경우 최초 구독이 발생할 때 connect()를 호출하도록 동작하고,
		publish().refCount()를 하나의 연산자로 추상화한 연산자를 share()라고 합니다.

		그 밖에 RxJava의 ConnectableFlowable도 동일한 기능을 제공하는 것을 문서를 통해 확인할 수 있었습니다.
	 */
	@Test
	public void fluxTestHotFlux() throws InterruptedException {
		final Flux<List<String>> basketFlux = getFruitsFlux();

		CountDownLatch countDownLatch = new CountDownLatch(1);

		basketFlux.concatMap(basket -> {
			// Hot 으로 변환된 ConnectableFlux를 최소 2개의 구독자가 구독을 하면 자동으로 구독하는 Flux를 리턴
			final Flux<String> source = Flux.fromIterable(basket).log().publish().autoConnect(2);

			/*
			subscribeOn(Schedulers.parallel()) 에 의해서
			처음엔 parallel-1과 parallel-2로 동작하는 듯했다가 결국 source는 parallel-1 에서 실행됩니다.
			2개의 구독자가 구독을 한 후 source가 parallel-1 에서 시작되니 그 이후 동작도 다 parallel-1에서 실행되는 것입니다.
			subscribeOn으로 스케줄러를 지정하여 스위칭이 되면 이후 스트림은 계속 그 스케줄러에 의해 동작되기 때문입니다.
			 */
			final Mono<List<String>> distinctFruits = source.distinct().collectList().log().subscribeOn(Schedulers.parallel());
			final Mono<Map<String, Long>> countFruitsMono = source
					.groupBy(fruit -> fruit)
					.concatMap(groupedFlux -> groupedFlux.count()
							.map(count -> {
								final Map<String, Long> fruitCount = new LinkedHashMap<>();
								fruitCount.put(groupedFlux.key(), count);
								return fruitCount;
							})
					)
					.reduce((accumulatedMap, currentMap) -> new LinkedHashMap<String, Long>() { {
						putAll(accumulatedMap);
						putAll(currentMap);
					}})
					.log()
					.subscribeOn(Schedulers.parallel());

			return Flux.zip(distinctFruits, countFruitsMono, (distinct, count) -> new FruitInfo(distinct, count));
		}).subscribe(s -> log.info(s.toString()), // 값이 넘어올 때 호출, onNext(T)
				error -> {
					log.error(error.getMessage());
					countDownLatch.countDown();
				}, // 에러 발생시 출력하고 countDown, onError(Throwable)
				() -> {
					log.info("complete");
					countDownLatch.countDown();
				} // 정상적 종료시 countDown, onComplete()
		);

		countDownLatch.await();
	}

	@Test
	public void fluxTestHotFluxAsyncPerSource() throws InterruptedException {
		final Flux<List<String>> basketFlux = getFruitsFlux();

		CountDownLatch countDownLatch = new CountDownLatch(1);

		basketFlux.concatMap(basket -> {
			// Hot 으로 변환된 ConnectableFlux를 최소 2개의 구독자가 구독을 하면 자동으로 구독하는 Flux를 리턴
			final Flux<String> source = Flux.fromIterable(basket).log().publish().autoConnect(2).subscribeOn(Schedulers.single());

			/*
				source는 single-1이라는 스레드에서 항상 동작하고,
				그 이후에는 각각 parallel-1과parallel-2, parallel-3과parallel-4, parallel-5과parallel-6으로 동작하는 것을 확인
			 */
			final Mono<List<String>> distinctFruits = source.publishOn(Schedulers.parallel()).distinct().collectList().log();
			final Mono<Map<String, Long>> countFruitsMono = source.publishOn(Schedulers.parallel())
					.groupBy(fruit -> fruit)
					.concatMap(groupedFlux -> groupedFlux.count()
							.map(count -> {
								final Map<String, Long> fruitCount = new LinkedHashMap<>();
								fruitCount.put(groupedFlux.key(), count);
								return fruitCount;
							})
					)
					.reduce((accumulatedMap, currentMap) -> new LinkedHashMap<String, Long>() { {
						putAll(accumulatedMap);
						putAll(currentMap);
					}})
					.log();

			return Flux.zip(distinctFruits, countFruitsMono, (distinct, count) -> new FruitInfo(distinct, count));
		}).subscribe(s -> log.info(s.toString()), // 값이 넘어올 때 호출, onNext(T)
				error -> {
					log.error(error.getMessage());
					countDownLatch.countDown();
				}, // 에러 발생시 출력하고 countDown, onError(Throwable)
				() -> {
					log.info("complete");
					countDownLatch.countDown();
				} // 정상적 종료시 countDown, onComplete()
		);

		countDownLatch.await();
	}

	private Flux<List<String>> getFruitsFlux() {
		final List<String> basket1 = Arrays.asList(new String[]{"kiwi", "orange", "lemon", "orange", "lemon", "kiwi"});
		final List<String> basket2 = Arrays.asList(new String[]{"banana", "lemon", "lemon", "kiwi"});
		final List<String> basket3 = Arrays.asList(new String[]{"strawberry", "orange", "lemon", "grape", "strawberry"});
		final List<List<String>> baskets = Arrays.asList(basket1, basket2, basket3);

		return Flux.fromIterable(baskets);
	}

	/*
		io.projectreactor:reactor-test를 의존성으로 추가하고 StepVerifier를 통해 테스트 코드를 작성할 수 있었습니다.
		StepVerifier.create로 테스트할 객체를 만들 때 인자로 테스트 대상이 되는 Flux나 Mono를 넘깁니다.
		그리고 테스트에 필요한 메서드들을 연달아서 호출해서 기대한 값이 나왔는지 확인할 수 있습니다.
		여기서는 expectNext와 verifyComplete를 이용해서 next로 넘어온 값이 기대한 값인지 그리고 complete이 호출되었는지 검증해보도록 하겠습니다.
	 */
	@Test
	public void verifyFlux() {
		final FruitInfo expected1 = new FruitInfo(
			Arrays.asList("kiwi", "orange", "lemon"),
			new LinkedHashMap<String, Long>() {
				{
					put("kiwi", 2L);
					put("orange", 2L);
					put("lemon", 2L);
				}
			}
		);

		final FruitInfo expected2 = new FruitInfo(
				Arrays.asList("banana", "lemon", "kiwi"),
				new LinkedHashMap<String, Long>() {
					{
						put("banana", 1L);
						put("lemon", 2L);
						put("kiwi", 1L);
					}
				}
		);

		final FruitInfo expected3 = new FruitInfo(
				Arrays.asList("strawberry", "orange", "lemon", "grape"),
				new LinkedHashMap<String, Long>() {
					{
						put("strawberry", 2L);
						put("orange", 1L);
						put("lemon", 1L);
						put("grape", 1L);
					}
				}
		);

		StepVerifier.create(getFluxList())
				.expectNext(expected1)
				.expectNext(expected2)
				.expectNext(expected3)
				.verifyComplete();
	}

	private Flux<FruitInfo> getFluxList() {
		final Flux<List<String>> basketFlux = getFruitsFlux();

		return basketFlux.concatMap(basket -> {
			final Flux<String> source = Flux.fromIterable(basket).log().publish().autoConnect(2).subscribeOn(Schedulers.single());

			final Mono<List<String>> distinctFruits = source.publishOn(Schedulers.parallel()).distinct().collectList().log();
			final Mono<Map<String, Long>> countFruitsMono = source.publishOn(Schedulers.parallel())
					.groupBy(fruit -> fruit)
					.concatMap(groupedFlux -> groupedFlux.count()
							.map(count -> {
								final Map<String, Long> fruitCount = new LinkedHashMap<>();
								fruitCount.put(groupedFlux.key(), count);
								return fruitCount;
							})
					)
					.reduce((accumulatedMap, currentMap) -> new LinkedHashMap<String, Long>() { {
						putAll(accumulatedMap);
						putAll(currentMap);
					}})
					.log();

			return Flux.zip(distinctFruits, countFruitsMono, (distinct, count) -> new FruitInfo(distinct, count));
		});
	}

	public class FruitInfo {
		private final List<String> distinctFruits;
		private final Map<String, Long> countFruits;

		public FruitInfo(List<String> distinctFruits, Map<String, Long> countFruits) {
			this.distinctFruits = distinctFruits;
			this.countFruits = countFruits;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			FruitInfo fruitInfo = (FruitInfo) o;

			if (distinctFruits != null ? !distinctFruits.equals(fruitInfo.distinctFruits) : fruitInfo.distinctFruits != null)
				return false;

			return countFruits != null ? countFruits.equals(fruitInfo.countFruits) : fruitInfo.countFruits == null;
		}

		@Override
		public int hashCode() {
			int result = distinctFruits != null ? distinctFruits.hashCode() : 0;
			result = 31 * result + (countFruits != null ? countFruits.hashCode() : 0);
			return result;
		}

		@Override
		public String toString() {
			return "FruitInfo{" +
					"distinctFruits=" + distinctFruits +
					", countFruits=" + countFruits +
					'}';
		}
	}
}
