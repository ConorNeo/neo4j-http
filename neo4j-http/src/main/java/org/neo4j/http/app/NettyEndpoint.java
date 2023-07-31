package org.neo4j.http.app;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.ByteBuf;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import org.neo4j.driver.*;
import org.neo4j.driver.types.TypeSystem;
import org.neo4j.http.config.ApplicationProperties;
import org.neo4j.http.db.*;
import org.neo4j.http.message.DefaultRequestFormatModule;
import org.neo4j.http.message.DefaultResponseModule;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.http.Http2SslContextSpec;
import reactor.netty.http.HttpProtocol;
import reactor.netty.http.client.HttpClient;
import reactor.netty.http.server.HttpServer;
import reactor.netty.http.server.HttpServerRequest;
import reactor.netty.http.server.HttpServerResponse;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpRequest;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.time.Duration;
import java.time.temporal.TemporalUnit;
import java.util.logging.Level;

public class NettyEndpoint {
    static ObjectMapper om = new ObjectMapper();
    static Neo4jAdapter neo4j;

    static Neo4jPrincipal principal = new Neo4jPrincipal("neo4j",
                              AuthTokens.basic("neo4j", "password"));
    private static SelfSignedCertificate ssc;
    private static Http2SslContextSpec httpSpec;


    public static void main(String[] args) throws IOException, InterruptedException, CertificateException, NoSuchAlgorithmException, KeyManagementException {
        System.out.println("Starting Server");

        Driver driver = GraphDatabase.driver(
                "bolt://localhost:7687",
                principal.authToken(),
                Config.builder()
                        .withLogging(Logging.console(Level.INFO))
                        .build());

        driver.verifyConnectivity();

        neo4j = new DefaultNeo4jAdapter(
                new ApplicationProperties(100, false, false),
                new DefaultQueryEvaluator(driver),
                driver,
                driver.executableQueryBookmarkManager());

        var jacksonRequestModule = new DefaultRequestFormatModule();
        var jacksonResponseModule = new DefaultResponseModule(TypeSystem.getDefault());
        om.registerModules(jacksonRequestModule, jacksonResponseModule);

        ssc = new SelfSignedCertificate();
        httpSpec = Http2SslContextSpec.forServer(ssc.certificate(), ssc.privateKey());

        var server = HttpServer
                .create()
                .host("localhost")
                .port(8080)
                .protocol(HttpProtocol.H2C)
                .route(routes ->
                        routes
                                .post("/stream", NettyEndpoint::handleStream)
                                .post("/goodbye", NettyEndpoint::handleGoodbye))
                .bindNow();

        System.out.println("Port Bound, " + server.port());

        request();

        server.onDispose().block();
        System.out.println("Shutting down");
    }

    private static Publisher<Void> handleGoodbye(HttpServerRequest request, HttpServerResponse response) {
        return request.receive().asString()
                .flatMap(x -> {
                    System.out.println(x);
                    try {
                        var query = om.readValue(x, AnnotatedQuery.Container.class);
                        var mono = neo4j.run(principal, "neo4j", query.value().get(0)).map(y -> {
                            try {
                                return new ObjectMapper().writer().writeValueAsString(y);
                            } catch (JsonProcessingException e) {
                                throw new RuntimeException(e);
                            }
                        });

                        return response.sendString(mono);
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    private static Publisher<Void> handleStream(HttpServerRequest request, HttpServerResponse response) {
        return request.receive().asString()
                .flatMap(x -> {
                    System.out.println(x);
                    try {
                        var query = om.readValue(x, AnnotatedQuery.Container.class);
//                        var flux = neo4j.stream(
//                                principal,
//                                "neo4j",
//                                query.value().get(0).value()).map(y -> {
//                            try {
//                                return new ObjectMapper().writer().writeValueAsString(y);
//                            } catch (JsonProcessingException e) {
//                                throw new RuntimeException(e);
//                            }
//                        });

                        return response.sendString(Flux.just("hey", "i", "just", "met", "you").delayElements(Duration.ofSeconds(1)));
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                });
    }
    
    static String query = "{\"statements\":[{\"statement\": \"CREATE (n $props) RETURN n\",\"parameters\": {\"props\": {\"name\": \"My Node\"}}}]}";

    public static void request(){
        HttpClient client = HttpClient.create()
                .http2Settings(x -> x.maxFrameSize(100000))
                .protocol(HttpProtocol.H2C);

        var response = client
                .post()
                .uri("http://localhost:8080/stream")
                .send((x, y) -> y.sendString(Flux.just(query)))
                .response((res, bytes) -> bytes.asString().flatMap(x -> {
                    System.out.println(x);
                    return Mono.just(x);
                }));

        Flux.from(response).blockLast();

        System.out.println("Printing response:");
        System.out.println(response);
    }
}


