package org.neo4j.http.app;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.neo4j.driver.*;
import org.neo4j.driver.types.TypeSystem;
import org.neo4j.http.config.ApplicationProperties;
import org.neo4j.http.db.*;
import org.neo4j.http.message.DefaultRequestFormatModule;
import org.neo4j.http.message.DefaultResponseModule;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;
import reactor.netty.http.Http2SettingsSpec;
import reactor.netty.http.server.HttpServer;
import reactor.netty.http.server.HttpServerRequest;
import reactor.netty.http.server.HttpServerResponse;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.Flow;
import java.util.logging.Level;

public class NettyEndpoint {
    static ObjectMapper om = new ObjectMapper();
    static Neo4jAdapter neo4j;

    static Neo4jPrincipal principal = new Neo4jPrincipal("neo4j",
                              AuthTokens.basic("neo4j", "password"));

    public static void main(String[] args) throws IOException, InterruptedException {
        System.out.println("Starting Server");

        Driver driver = GraphDatabase.driver("bolt://localhost:7687", principal.authToken(), Config.builder().withLogging(Logging.console(Level.INFO)).build());

        driver.verifyConnectivity();

        neo4j = new DefaultNeo4jAdapter(
                new ApplicationProperties(100, false, false),
                new DefaultQueryEvaluator(driver),
                driver,
                driver.executableQueryBookmarkManager());

        var jacksonRequestModule = new DefaultRequestFormatModule();
        var jacksonResponseModule = new DefaultResponseModule(TypeSystem.getDefault());
        om.registerModules(jacksonRequestModule, jacksonResponseModule);

        DisposableServer server = HttpServer.create()
                .host("localhost")
                .port(8080)
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
                        var flux = neo4j.stream(principal, "neo4j", query.value().get(0).value()).map(y -> {
                            try {
                                return new ObjectMapper().writer().writeValueAsString(y);
                            } catch (JsonProcessingException e) {
                                throw new RuntimeException(e);
                            }
                        });

                        return response.sendString(flux);
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    public static void request() throws IOException, InterruptedException {
        var client = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_2)
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build();

        var response = client.send(HttpRequest.newBuilder().POST(HttpRequest.BodyPublishers.ofString("    {\n" +
                "        \"statements\": [\n" +
                "    {\n" +
                "      \"statement\": \"CREATE (n $props) RETURN n\",\n" +
                "      \"parameters\": {\n" +
                "        \"props\": {\n" +
                "          \"name\": \"My Node\"\n" +
                "        }\n" +
                "      }\n" +
                "    },\n" +
                "    {\n" +
                "      \"statement\": \"CREATE (n $props) RETURN n\",\n" +
                "      \"parameters\": {\n" +
                "        \"props\": {\n" +
                "          \"name\": \"Another Node\"\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  ]\n" +
                "    }")).uri(URI.create("http://localhost:8080/goodbye")).build(), HttpResponse.BodyHandlers.ofString());

        System.out.println("Printing response:");
        System.out.println(response.version());
        System.out.println(response.body());
    }
}
