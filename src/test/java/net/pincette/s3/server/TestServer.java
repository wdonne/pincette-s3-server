package net.pincette.s3.server;

import static com.typesafe.config.ConfigFactory.defaultApplication;
import static com.typesafe.config.ConfigValueFactory.fromAnyRef;
import static com.typesafe.config.ConfigValueFactory.fromIterable;
import static java.lang.String.valueOf;
import static java.net.http.HttpClient.Version.HTTP_1_1;
import static java.net.http.HttpClient.newBuilder;
import static java.net.http.HttpRequest.BodyPublishers.fromPublisher;
import static java.net.http.HttpRequest.BodyPublishers.noBody;
import static java.net.http.HttpResponse.BodyHandlers.discarding;
import static java.net.http.HttpResponse.BodyHandlers.ofByteArray;
import static java.net.http.HttpResponse.BodyHandlers.ofString;
import static java.nio.channels.FileChannel.open;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.READ;
import static java.time.Instant.now;
import static java.util.Objects.requireNonNull;
import static net.pincette.io.StreamConnector.copy;
import static net.pincette.json.Factory.f;
import static net.pincette.json.Factory.o;
import static net.pincette.json.Factory.v;
import static net.pincette.json.JsonUtil.createValue;
import static net.pincette.json.JsonUtil.from;
import static net.pincette.json.JsonUtil.string;
import static net.pincette.rs.Chain.with;
import static net.pincette.rs.ReadableByteChannelPublisher.readableByteChannel;
import static net.pincette.s3.server.Server.ACL;
import static net.pincette.util.Collections.list;
import static net.pincette.util.Collections.map;
import static net.pincette.util.MimeType.getContentTypeFromName;
import static net.pincette.util.Pair.pair;
import static net.pincette.util.Util.tryToDoRethrow;
import static net.pincette.util.Util.tryToGetRethrow;
import static net.pincette.util.Util.tryToGetWithRethrow;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.auth0.jwt.JWT;
import com.typesafe.config.Config;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Stream;
import javax.json.JsonObject;
import javax.json.JsonValue;
import net.pincette.io.DevNullInputStream;
import net.pincette.io.PathUtil;
import net.pincette.json.JsonUtil;
import net.pincette.jwt.Signer;
import net.pincette.rs.Source;
import net.pincette.rs.multipart.BodyPart;
import net.pincette.rs.multipart.MultipartEncoder;
import net.pincette.util.Pair;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TestServer {
  private static final String ACL_READ = "read";
  private static final String ACL_WRITE = "write";
  private static final String AUTHORIZATION = "Authorization";
  private static final String BOUNDARY = "2982c546-0d24-4738-b21c-116fc18819cd";
  private static final String CONTENT_DISPOSITION = "Content-Disposition";
  private static final String CONTENT_LENGTH = "Content-Length";
  private static final String CONTENT_TYPE = "Content-Type";
  private static final String DEFAULT_PATH = "/folder/";
  private static final String PATH = "/test/folder/";
  private static final String READ_ROLE = "read-role";
  private static final String ROLE = "role";
  private static final String WRITE_ROLE = "write-role";

  private static final File directory = new File("/tmp/files");
  private static final Map<String, byte[]> resources = new HashMap<>();
  private static final Server server = new Server().withConfig(config()).withPort(9000);
  private static final Signer signer = new Signer(readKey("rsa.priv"));
  private static final URI uri = URI.create("http://localhost:9000");

  private static JsonObject acl(final List<String> read, final List<String> write) {
    return o(f(ACL, o(f(ACL_READ, v(createValue(read))), f(ACL_WRITE, v(createValue(write))))));
  }

  @AfterAll
  static void afterAll() {
    PathUtil.delete(directory.toPath());
    server.close();
  }

  @BeforeAll
  static void beforeAll() {
    if (directory.exists()) {
      PathUtil.delete(directory.toPath());
    }

    directory.mkdir();
  }

  private static BodyPart bodyPart(final File file) {
    return tryToGetRethrow(
            () ->
                new BodyPart(
                    map(
                        pair(CONTENT_TYPE, new String[] {getContentTypeFromName(file.getName())}),
                        pair(CONTENT_LENGTH, new String[] {valueOf(file.length())}),
                        pair(
                            CONTENT_DISPOSITION,
                            new String[] {
                              "form-data ; name=\"files\"; filename=\"" + file.getName() + "\""
                            })),
                    readableByteChannel(open(file.toPath(), READ), true, 1024)))
        .orElse(null);
  }

  private static void checkFile(final File file, final List<String> roles) {
    checkFile(file, roles, false);
  }

  private static void checkFile(final File file, final List<String> roles, final boolean local) {
    final Pair<Integer, byte[]> result = get(local ? file.getAbsolutePath() : toPath(file), roles);

    assertEquals(200, result.first);
    assertArrayEquals(
        read(() -> tryToGetRethrow(() -> new FileInputStream(file)).orElse(null)), result.second);
  }

  private static HttpClient client() {
    return newBuilder().version(HTTP_1_1).build();
  }

  private static Config config() {
    return defaultApplication()
        .withValue("jwtPublicKey", fromAnyRef(readKey("rsa.pub")))
        .withValue("buckets.test.name", fromAnyRef("lars-tst-docs"))
        .withValue("buckets.test.roles", fromIterable(list("role")))
        .withValue("buckets.default.name", fromAnyRef("lars-tst-docs"))
        .withValue("buckets.default.roles", fromIterable(list("role")))
        .withValue("localPaths", fromIterable(list(directory.getAbsolutePath())));
  }

  private static File copyResource(final String resource) {
    final File file = new File(directory, resource);

    tryToDoRethrow(
        () ->
            copy(
                requireNonNull(TestServer.class.getResourceAsStream("/files/" + resource)),
                new FileOutputStream(requireNonNull(file))));

    return file;
  }

  private static int delete(final String path, final List<String> roles) {
    return tryToGetWithRethrow(
            TestServer::client,
            client -> client.send(request(path, roles).DELETE().build(), ofString()).statusCode())
        .orElse(500);
  }

  private static void deleteAndCheck(final File file, final List<String> roles) {
    assertEquals(204, delete(toPath(file), roles));
  }

  private static Pair<Integer, byte[]> get(final String path, final List<String> roles) {
    return tryToGetWithRethrow(
            TestServer::client,
            client -> client.send(request(path, roles).GET().build(), ofByteArray()))
        .map(
            response ->
                pair(response.statusCode(), response.statusCode() == 200 ? response.body() : null))
        .orElseGet(() -> pair(500, null));
  }

  private static Pair<Integer, JsonObject> getMetadata(
      final String path, final List<String> roles) {
    return tryToGetWithRethrow(
            TestServer::client,
            client ->
                client.send(
                    request(path + ";metadata", roles).GET().build(), BodyHandlers.ofString(UTF_8)))
        .filter(response -> response.statusCode() == 200)
        .flatMap(
            response ->
                from(response.body())
                    .filter(JsonUtil::isObject)
                    .map(JsonValue::asJsonObject)
                    .map(b -> pair(response.statusCode(), response.statusCode() == 200 ? b : null)))
        .orElseGet(() -> pair(500, null));
  }

  private static List<File> files() {
    return Stream.of("file1.txt", "file2.txt", "file3.pdf", "file4")
        .map(TestServer::copyResource)
        .toList();
  }

  private static HttpResponse<Void> head(final String path, final List<String> roles) {
    return tryToGetWithRethrow(
            TestServer::client,
            client ->
                client.send(request(path, roles).method("HEAD", noBody()).build(), discarding()))
        .orElse(null);
  }

  private static int put(final File file, final String path, final List<String> roles) {
    return tryToGetWithRethrow(
            TestServer::client,
            client ->
                client
                    .send(
                        request(path, roles)
                            .header(CONTENT_TYPE, getContentTypeFromName(file.getName()))
                            .PUT(BodyPublishers.ofFile(file.toPath()))
                            .build(),
                        ofString())
                    .statusCode())
        .orElse(500);
  }

  private static byte[] read(final String resource) {
    return resources.computeIfAbsent(
        resource,
        r ->
            read(
                () ->
                    tryToGetRethrow(() -> TestServer.class.getResourceAsStream(r))
                        .orElseGet(DevNullInputStream::new)));
  }

  private static byte[] read(final Supplier<InputStream> in) {
    final ByteArrayOutputStream out = new ByteArrayOutputStream(0xfffff);

    tryToDoRethrow(() -> copy(in.get(), out));

    return out.toByteArray();
  }

  private static String readKey(final String name) {
    return new String(read("/" + name), US_ASCII);
  }

  private static HttpRequest.Builder request(final String path, final List<String> roles) {
    return HttpRequest.newBuilder()
        .uri(uri.resolve(path))
        .header(AUTHORIZATION, "Bearer " + token(roles));
  }

  private static int setMetadata(
      final String path, final JsonObject metadata, final List<String> roles) {
    return tryToGetWithRethrow(
            TestServer::client,
            client ->
                client
                    .send(
                        request(path, roles)
                            .header(CONTENT_TYPE, "application/json")
                            .POST(BodyPublishers.ofString(string(metadata), UTF_8))
                            .build(),
                        ofString())
                    .statusCode())
        .orElse(500);
  }

  private static String toPath(final File file) {
    return toPath(file, true);
  }

  private static String toPath(final File file, final boolean named) {
    return (named ? PATH : DEFAULT_PATH) + file.getName();
  }

  private static String token(final List<String> roles) {
    return signer.sign(
        JWT.create().withClaim("roles", roles).withExpiresAt(now().plusSeconds(3600)));
  }

  private static int upload(final List<File> files, final String path, final List<String> roles) {
    return tryToGetWithRethrow(
            TestServer::client,
            client ->
                client
                    .send(
                        request(path, roles)
                            .header(CONTENT_TYPE, "multipart/form-data; boundary=" + BOUNDARY)
                            .POST(
                                fromPublisher(
                                    with(Source.of(
                                            files.stream().map(TestServer::bodyPart).toList()))
                                        .map(new MultipartEncoder(BOUNDARY))
                                        .get()))
                            .build(),
                        ofString())
                    .statusCode())
        .orElse(500);
  }

  @Test
  @DisplayName("acl")
  void acl() {
    final File file = copyResource("file1.txt");
    final String path = toPath(file);

    delete(path, list(ROLE, WRITE_ROLE));
    assertEquals(200, put(file, path, list(ROLE)));
    checkFile(file, list(ROLE));
    assertEquals(200, setMetadata(path, acl(list(READ_ROLE), list(WRITE_ROLE)), list(ROLE)));
    assertEquals(403, get(path, list(ROLE)).first);
    assertEquals(200, get(path, list(ROLE, READ_ROLE)).first);
    assertEquals(403, setMetadata(path, acl(list(READ_ROLE), list(WRITE_ROLE)), list(ROLE)));
    assertEquals(
        200, setMetadata(path, acl(list(READ_ROLE), list(WRITE_ROLE)), list(ROLE, WRITE_ROLE)));
    assertEquals(403, put(file, path, list(ROLE)));
    assertEquals(403, delete(path, list(ROLE)));
    deleteAndCheck(file, list(ROLE, WRITE_ROLE));
  }

  @Test
  @DisplayName("basic")
  void basic() {
    final File file = copyResource("file1.txt");
    final String path = toPath(file);
    final List<String> roles = list(ROLE);

    assertEquals(200, put(file, path, roles));
    checkFile(file, roles);
    deleteAndCheck(file, roles);
  }

  @Test
  @DisplayName("head")
  void head() {
    final File file = copyResource("file1.txt");
    final String path = toPath(file, false);
    final List<String> roles = list(ROLE);

    assertEquals(200, put(file, path, roles));

    final HttpResponse<Void> response = head(path, roles);

    assertEquals(200, response.statusCode());
    assertEquals("text/plain", response.headers().firstValue(CONTENT_TYPE).orElse(null));
    assertEquals(204, delete(path, roles));
  }

  @Test
  @DisplayName("local get")
  void localGet() {
    checkFile(copyResource("file1.txt"), list(ROLE), true);
  }

  @Test
  @DisplayName("local head")
  void localHead() {
    final File file = copyResource("file1.txt");
    final HttpResponse<Void> response = head(file.getAbsolutePath(), list(ROLE));

    assertEquals(200, response.statusCode());
    assertEquals("text/plain", response.headers().firstValue(CONTENT_TYPE).orElse(null));
    assertEquals(
        file.length(),
        response.headers().firstValue(CONTENT_LENGTH).map(Long::parseLong).orElse(0L));
  }

  @Test
  @DisplayName("no global role")
  void noGlobalRole() {
    final File file = copyResource("file1.txt");
    final String path = toPath(file);

    assertEquals(403, put(file, path, list()));
    assertEquals(200, put(file, path, list(ROLE)));

    final Pair<Integer, byte[]> result = get(path, list());

    assertEquals(403, result.first);
    assertEquals(403, delete(path, list()));
    deleteAndCheck(file, list(ROLE));
  }

  @Test
  @DisplayName("upload")
  void upload() {
    final List<File> files = files();

    assertEquals(200, upload(files, PATH, list(ROLE)));
    files.forEach(
        f -> {
          checkFile(f, list(ROLE));
          deleteAndCheck(f, list(ROLE));
        });
  }
}
