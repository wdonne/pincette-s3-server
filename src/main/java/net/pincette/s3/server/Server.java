package net.pincette.s3.server;

import static io.netty.buffer.Unpooled.wrappedBuffer;
import static io.netty.handler.codec.http.HttpHeaderNames.ACCEPT_RANGES;
import static io.netty.handler.codec.http.HttpHeaderNames.CACHE_CONTROL;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_DISPOSITION;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_ENCODING;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_LANGUAGE;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_LENGTH;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpHeaderNames.ETAG;
import static io.netty.handler.codec.http.HttpHeaderNames.EXPIRES;
import static io.netty.handler.codec.http.HttpHeaderNames.IF_MATCH;
import static io.netty.handler.codec.http.HttpHeaderNames.IF_MODIFIED_SINCE;
import static io.netty.handler.codec.http.HttpHeaderNames.IF_NONE_MATCH;
import static io.netty.handler.codec.http.HttpHeaderNames.IF_UNMODIFIED_SINCE;
import static io.netty.handler.codec.http.HttpHeaderNames.LAST_MODIFIED;
import static io.netty.handler.codec.http.HttpHeaderNames.RANGE;
import static io.netty.handler.codec.http.HttpMethod.DELETE;
import static io.netty.handler.codec.http.HttpMethod.GET;
import static io.netty.handler.codec.http.HttpMethod.HEAD;
import static io.netty.handler.codec.http.HttpMethod.POST;
import static io.netty.handler.codec.http.HttpMethod.PUT;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_IMPLEMENTED;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static java.lang.Long.parseLong;
import static java.lang.String.valueOf;
import static java.net.URLDecoder.decode;
import static java.nio.ByteBuffer.wrap;
import static java.nio.channels.FileChannel.open;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.Instant.ofEpochMilli;
import static java.util.Arrays.stream;
import static java.util.Optional.ofNullable;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.joining;
import static net.pincette.config.Util.configValue;
import static net.pincette.json.JsonUtil.createObjectBuilder;
import static net.pincette.json.JsonUtil.createValue;
import static net.pincette.json.JsonUtil.emptyObject;
import static net.pincette.json.JsonUtil.from;
import static net.pincette.json.JsonUtil.getArray;
import static net.pincette.json.JsonUtil.getValue;
import static net.pincette.json.JsonUtil.string;
import static net.pincette.netty.http.PipelineHandler.handle;
import static net.pincette.netty.http.Util.getBearerToken;
import static net.pincette.netty.http.Util.simpleResponse;
import static net.pincette.netty.http.Util.wrapTracing;
import static net.pincette.rs.Chain.with;
import static net.pincette.rs.LambdaSubscriber.lambdaSubscriber;
import static net.pincette.rs.Util.asValueAsync;
import static net.pincette.rs.Util.empty;
import static net.pincette.rs.json.Util.parseJson;
import static net.pincette.s3.server.Application.LOGGER;
import static net.pincette.s3.util.Util.deleteObject;
import static net.pincette.s3.util.Util.deleteObjectRequest;
import static net.pincette.s3.util.Util.getObject;
import static net.pincette.s3.util.Util.headObject;
import static net.pincette.s3.util.Util.putObject;
import static net.pincette.util.Collections.intersection;
import static net.pincette.util.Collections.map;
import static net.pincette.util.ImmutableBuilder.create;
import static net.pincette.util.MimeType.getContentTypeFromName;
import static net.pincette.util.Pair.pair;
import static net.pincette.util.StreamUtil.tail;
import static net.pincette.util.Util.getLastSegment;
import static net.pincette.util.Util.getSegments;
import static net.pincette.util.Util.getStackTrace;
import static net.pincette.util.Util.tryToGet;
import static net.pincette.util.Util.tryToGetRethrow;
import static net.pincette.util.Util.tryToGetSilent;

import com.typesafe.config.Config;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.io.File;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow.Publisher;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;
import javax.json.JsonObject;
import javax.json.JsonValue;
import net.pincette.json.JsonUtil;
import net.pincette.netty.http.HeaderHandler;
import net.pincette.netty.http.HttpServer;
import net.pincette.netty.http.JWTVerifier;
import net.pincette.netty.http.RequestHandler;
import net.pincette.rs.ReadableByteChannelPublisher;
import net.pincette.rs.Source;
import net.pincette.rs.multipart.MultipartDecoder;
import net.pincette.s3.util.Util;
import net.pincette.util.Builder;
import net.pincette.util.Cases;
import software.amazon.awssdk.core.SdkResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * An S3 webserver.
 *
 * @author Werner Donné
 */
public class Server {
  static final String ACL = "_acl";
  private static final String BOUNDARY = "boundary";
  private static final String BUCKETS = "buckets";
  private static final String DEFAULT = "default";
  private static final String FILENAME = "filename";
  private static final String FORM_DATA = "multipart/form-data";
  private static final DateTimeFormatter HTTP_DATE_FORMAT =
      DateTimeFormatter.ofPattern("EEE, dd MMM yyyy HH:mm:ss zzz").withZone(ZoneId.of("GMT"));
  private static final String JSON = "application/json";
  private static final String JWT_PUBLIC_KEY = "jwtPublicKey";
  private static final String JWT_ROLES_FIELD = "jwtRolesField";
  private static final String LOCAL_PATHS = "localPaths";
  private static final String METADATA = "metadata";
  private static final String NAME = "name";
  private static final String OCTET_STREAM = "application/octet-stream";
  private static final String READ = "read";
  private static final String ROLES = "roles";
  private static final String WRITE = "write";

  private final Config config;
  private final HttpServer httpServer;
  private final int port;

  private Server(final int port, final Config config) {
    this.port = port;
    this.config = config;
    httpServer =
        port != -1 && config != null
            ? new HttpServer(
                port, wrapTracing(handle(headerHandler(config)).finishWith(handler()), LOGGER))
            : null;
  }

  public Server() {
    this(-1, null);
  }

  private static Publisher<ByteBuf> badRequest(final HttpResponse response) {
    return emptyResult(response, BAD_REQUEST);
  }

  private static Optional<Config> bucketConfig(final String path, final Config config) {
    return getSegments(path, "/")
        .findFirst()
        .map(
            segment ->
                configValue(config::getConfig, BUCKETS + "." + segment)
                    .orElseGet(
                        () ->
                            configValue(config::getConfig, BUCKETS + "." + DEFAULT).orElse(null)));
  }

  private static long contentLength(final Map<String, String[]> headers) {
    return ofNullable(headers.get(CONTENT_LENGTH.toString()))
        .filter(values -> values.length > 0)
        .map(values -> parseLong(values[0]))
        .orElse(-1L);
  }

  private static String contentType(final Map<String, String[]> headers) {
    return ofNullable(headers.get(CONTENT_TYPE.toString()))
        .filter(values -> values.length > 0)
        .map(values -> values[0])
        .orElse(OCTET_STREAM);
  }

  private static CompletionStage<Publisher<ByteBuf>> delete(
      final HttpResponse response, final String bucket, final String key) {
    return deleteObject(deleteObjectRequest(bucket, key))
        .thenApply(resp -> deleteResponse(resp, response));
  }

  private static Publisher<ByteBuf> deleteResponse(
      final DeleteObjectResponse s3Response, final HttpResponse response) {
    return emptyResult(response, status(s3Response));
  }

  private static Publisher<ByteBuf> emptyResult(
      final HttpResponse response, final HttpResponseStatus status) {
    response.setStatus(status);
    response.headers().remove(CONTENT_LENGTH.toString());

    return empty();
  }

  private static Publisher<ByteBuf> exception(final HttpResponse response, final Throwable t) {
    response.setStatus(INTERNAL_SERVER_ERROR);

    return Source.of(wrappedBuffer(getStackTrace(t).getBytes(UTF_8)));
  }

  private static Optional<String> filename(final Map<String, String[]> headers) {
    return ofNullable(headers.get(CONTENT_DISPOSITION.toString()))
        .filter(value -> value.length == 1)
        .map(value -> headerParameters(value[0]))
        .map(parameters -> parameters.get(FILENAME))
        .flatMap(filename -> getLastSegment(filename, "/"));
  }

  private static Publisher<ByteBuf> forbidden(final HttpResponse response) {
    return emptyResult(response, FORBIDDEN);
  }

  private static CompletionStage<Publisher<ByteBuf>> get(
      final HttpRequest request,
      final HttpResponse response,
      final String bucket,
      final String key,
      final Config config) {
    final Function<Throwable, Publisher<ByteBuf>> ex =
        t ->
            t.getCause() != null && t.getCause() instanceof S3Exception s3Exception
                ? handleException(s3Exception, response)
                : exception(response, t);

    return isMetadata(request)
        ? getMetadata(request, response, bucket, key, config)
        : getObject(getObjectRequest(request, bucket, key))
            .thenApply(pair -> getResponse(pair.first, pair.second, request, response, config))
            .exceptionally(ex);
  }

  private static Optional<String> getBoundary(final String contentType) {
    return ofNullable(headerParameters(contentType).get(BOUNDARY));
  }

  private static Publisher<ByteBuffer> getLocalFile(final File file) {
    return tryToGetRethrow(() -> open(file.toPath(), StandardOpenOption.READ))
        .map(ReadableByteChannelPublisher::new)
        .map(p -> (Publisher<ByteBuffer>) p)
        .orElseGet(net.pincette.rs.Util::empty);
  }

  private static CompletionStage<Publisher<ByteBuf>> getMetadata(
      final HttpRequest request,
      final HttpResponse response,
      final String bucket,
      final String key,
      final Config config) {
    return headObject(bucket, key)
        .thenApply(resp -> getMetadataResponse(resp, request, response, config));
  }

  private static Publisher<ByteBuf> getMetadataResponse(
      final HeadObjectResponse s3Response,
      final HttpRequest request,
      final HttpResponse response,
      final Config config) {
    final HttpResponse resp = setHeaders(response, s3Response);
    final Supplier<Publisher<ByteBuf>> normal =
        () ->
            s3Response.sdkHttpResponse().isSuccessful()
                ? ok(resp, Source.of(wrap(string(toJson(s3Response.metadata())).getBytes(UTF_8))))
                : emptyResult(resp, status(s3Response));

    resp.headers().set(CONTENT_TYPE, JSON);

    return isAllowedToRead(getRoles(request, config), toJson(s3Response.metadata()))
        ? normal.get()
        : forbidden(response);
  }

  private static GetObjectRequest getObjectRequest(
      final HttpRequest request, final String bucket, final String key) {
    return setHeaders(request, GetObjectRequest.builder().bucket(bucket).key(key)).build();
  }

  private static Publisher<ByteBuf> getResponse(
      final GetObjectResponse s3Response,
      final Publisher<ByteBuffer> stream,
      final HttpRequest request,
      final HttpResponse response,
      final Config config) {
    final HttpResponse resp = setHeaders(response, s3Response);
    final Supplier<Publisher<ByteBuf>> normal =
        () ->
            s3Response.sdkHttpResponse().isSuccessful()
                ? ok(resp, stream)
                : emptyResult(resp, status(s3Response));

    return isAllowedToRead(getRoles(request, config), toJson(s3Response.metadata()))
        ? normal.get()
        : forbidden(response);
  }

  private static List<String> getRoles(final HttpRequest request, final Config config) {
    return getBearerToken(request)
        .flatMap(net.pincette.jwt.Util::getJwtPayload)
        .flatMap(json -> getValue(json, "/" + rolesField(config)))
        .filter(JsonUtil::isArray)
        .map(JsonValue::asJsonArray)
        .map(JsonUtil::strings)
        .map(Stream::toList)
        .orElseGet(Collections::emptyList);
  }

  private static Publisher<ByteBuf> handleException(
      final S3Exception exception, final HttpResponse response) {
    final int statusCode = exception.awsErrorDetails().sdkHttpResponse().statusCode();

    if (statusCode < 500) {
      response.setStatus(HttpResponseStatus.valueOf(statusCode));
      setHeaders(response, exception.awsErrorDetails().sdkHttpResponse().headers());

      return empty();
    }

    return exception(response, exception);
  }

  private static CompletionStage<Publisher<ByteBuf>> head(
      final HttpRequest request,
      final HttpResponse response,
      final String bucket,
      final String key,
      final Config config) {
    return headObject(bucket, key)
        .thenApply(
            resp ->
                isAllowedToRead(getRoles(request, config), toJson(resp.metadata()))
                    ? ok(setHeaders(response, resp))
                    : forbidden(response));
  }

  private static HeaderHandler headerHandler(final Config config) {
    return configValue(config::getString, JWT_PUBLIC_KEY).map(JWTVerifier::verify).orElse(h -> h);
  }

  private static Map<String, String> headerParameters(final String value) {
    return map(
        stream(value.split(";"))
            .map(String::trim)
            .map(s -> s.split("="))
            .filter(parts -> parts.length == 1 || parts.length == 2)
            .map(
                parts ->
                    pair(
                        parts[0].trim(),
                        parts.length == 2 ? decode(unquote(parts[1].trim()), UTF_8) : "")));
  }

  private static boolean isAllowed(
      final List<String> roles, final JsonObject metadata, final String action) {
    return getArray(metadata, "/" + ACL + "/" + action)
        .map(JsonUtil::strings)
        .map(Stream::toList)
        .map(values -> !intersection(values, roles).isEmpty())
        .orElse(true);
  }

  private static boolean isAllowedToRead(final List<String> roles, final JsonObject metadata) {
    return isAllowed(roles, metadata, READ);
  }

  private static boolean isAllowedToWrite(final List<String> roles, final JsonObject metadata) {
    return isAllowed(roles, metadata, WRITE);
  }

  private static boolean isDefault(final String path, final Config config) {
    return !getSegments(path, "/")
            .findFirst()
            .map(segment -> config.hasPath(BUCKETS + "." + segment))
            .orElse(false)
        && config.hasPath(BUCKETS + "." + DEFAULT);
  }

  private static boolean isMetadata(final HttpRequest request) {
    return request.uri().endsWith(";" + METADATA);
  }

  private static String key(final String path, final Config config) {
    final Supplier<String> trailing = () -> path.endsWith("/") ? "/" : "";

    return removeLeadingSlash(
        isDefault(path, config)
            ? path
            : (tail(getSegments(stripMetadata(path), "/")).collect(joining("/")) + trailing.get()));
  }

  private static Optional<File> localFile(final HttpRequest request) {
    return path(request).map(File::new).filter(File::canRead);
  }

  private static Publisher<ByteBuf> notFound(final HttpResponse response) {
    return emptyResult(response, NOT_FOUND);
  }

  private static Publisher<ByteBuf> ok(
      final HttpResponse response, final Publisher<ByteBuffer> body) {
    response.setStatus(OK);

    return with(body).map(Unpooled::wrappedBuffer).get();
  }

  private static Publisher<ByteBuf> ok(final HttpResponse response) {
    return emptyResult(response, OK);
  }

  private static Instant parseHttpDate(final String date) {
    return Instant.from(HTTP_DATE_FORMAT.parse(date));
  }

  private static Optional<String> path(final HttpRequest request) {
    return tryToGet(() -> new URI(request.uri()).getPath())
        .map(
            path ->
                path.endsWith("/")
                        && (request.method().equals(GET) || request.method().equals(HEAD))
                    ? (path + "index.html")
                    : path);
  }

  private static CompletionStage<Publisher<ByteBuf>> put(
      final HttpRequest request,
      final Publisher<ByteBuf> requestBody,
      final HttpResponse response,
      final String bucket,
      final String key) {
    return putObject(
            putObjectRequest(request, bucket, key), with(requestBody).map(ByteBuf::nioBuffer).get())
        .thenApply(resp -> putResponse(resp, response));
  }

  private static PutObjectRequest putObjectRequest(
      final HttpRequest request, final String bucket, final String key) {
    return setHeaders(request, PutObjectRequest.builder().bucket(bucket).key(key)).build();
  }

  private static Publisher<ByteBuf> putResponse(
      final PutObjectResponse s3Response, final HttpResponse response) {
    return emptyResult(setHeaders(response, s3Response), status(s3Response));
  }

  private static CompletionStage<JsonObject> readJson(final Publisher<ByteBuf> body) {
    return asValueAsync(
        with(body)
            .map(ByteBuf::nioBuffer)
            .map(parseJson())
            .filter(JsonUtil::isObject)
            .map(JsonValue::asJsonObject)
            .get());
  }

  private static String removeLeadingSlash(final String path) {
    return path.startsWith("/") ? path.substring(1) : path;
  }

  private static String rolesField(final Config config) {
    return tryToGetSilent(() -> config.getString(JWT_ROLES_FIELD)).orElse(ROLES);
  }

  private static GetObjectRequest.Builder setHeaders(
      final HttpRequest request, final GetObjectRequest.Builder builder) {
    return create(() -> builder)
        .updateIf(
            () -> ofNullable(request.headers().get(IF_MODIFIED_SINCE)).map(Server::parseHttpDate),
            GetObjectRequest.Builder::ifModifiedSince)
        .updateIf(
            () -> ofNullable(request.headers().get(IF_UNMODIFIED_SINCE)).map(Server::parseHttpDate),
            GetObjectRequest.Builder::ifUnmodifiedSince)
        .updateIf(
            () -> ofNullable(request.headers().get(IF_MATCH)), GetObjectRequest.Builder::ifMatch)
        .updateIf(
            () -> ofNullable(request.headers().get(IF_NONE_MATCH)),
            GetObjectRequest.Builder::ifNoneMatch)
        .updateIf(() -> ofNullable(request.headers().get(RANGE)), GetObjectRequest.Builder::range)
        .build();
  }

  private static PutObjectRequest.Builder setHeaders(
      final HttpRequest request, final PutObjectRequest.Builder builder) {
    return create(() -> builder)
        .updateIf(
            () -> ofNullable(request.headers().get(CONTENT_DISPOSITION)),
            PutObjectRequest.Builder::contentDisposition)
        .updateIf(
            () -> ofNullable(request.headers().get(CONTENT_ENCODING)),
            PutObjectRequest.Builder::contentEncoding)
        .updateIf(
            () -> ofNullable(request.headers().get(CONTENT_LANGUAGE)),
            PutObjectRequest.Builder::contentLanguage)
        .updateIf(
            () ->
                ofNullable(request.headers().getInt(CONTENT_LENGTH))
                    .filter(l -> l != -1)
                    .map(i -> (long) i),
            PutObjectRequest.Builder::contentLength)
        .updateIf(
            () -> ofNullable(request.headers().get(CONTENT_TYPE)),
            PutObjectRequest.Builder::contentType)
        .build();
  }

  private static HttpResponse setHeaders(
      final HttpResponse httpResponse, final GetObjectResponse s3Response) {
    return Builder.create(() -> httpResponse)
        .update(
            o -> o.headers().set(LAST_MODIFIED, HTTP_DATE_FORMAT.format(s3Response.lastModified())))
        .updateIf(
            () -> Optional.of(s3Response.contentLength()).filter(l -> l != -1),
            (o, v) -> o.headers().set(CONTENT_LENGTH, valueOf(v)))
        .updateIf(() -> ofNullable(s3Response.eTag()), (o, v) -> o.headers().add(ETAG, v))
        .updateIf(
            () -> ofNullable(s3Response.cacheControl()),
            (o, v) -> o.headers().add(CACHE_CONTROL, v))
        .updateIf(
            () -> ofNullable(s3Response.contentDisposition()),
            (o, v) -> o.headers().add(CONTENT_DISPOSITION, v))
        .updateIf(
            () -> ofNullable(s3Response.contentEncoding()),
            (o, v) -> o.headers().add(CONTENT_ENCODING, v))
        .updateIf(
            () -> ofNullable(s3Response.contentLanguage()),
            (o, v) -> o.headers().add(CONTENT_LANGUAGE, v))
        .updateIf(
            () -> ofNullable(s3Response.contentType()), (o, v) -> o.headers().add(CONTENT_TYPE, v))
        .updateIf(
            () -> ofNullable(s3Response.acceptRanges()),
            (o, v) -> o.headers().add(ACCEPT_RANGES, v))
        .updateIf(
            () -> ofNullable(s3Response.expiresString()), (o, v) -> o.headers().add(EXPIRES, v))
        .build();
  }

  private static HttpResponse setHeaders(
      final HttpResponse httpResponse, final HeadObjectResponse s3Response) {
    return Builder.create(() -> httpResponse)
        .update(
            o -> o.headers().set(LAST_MODIFIED, HTTP_DATE_FORMAT.format(s3Response.lastModified())))
        .updateIf(
            () -> Optional.of(s3Response.contentLength()).filter(l -> l != -1),
            (o, v) -> o.headers().set(CONTENT_LENGTH, valueOf(v)))
        .updateIf(() -> ofNullable(s3Response.eTag()), (o, v) -> o.headers().add(ETAG, v))
        .updateIf(
            () -> ofNullable(s3Response.cacheControl()),
            (o, v) -> o.headers().add(CACHE_CONTROL, v))
        .updateIf(
            () -> ofNullable(s3Response.contentDisposition()),
            (o, v) -> o.headers().add(CONTENT_DISPOSITION, v))
        .updateIf(
            () -> ofNullable(s3Response.contentEncoding()),
            (o, v) -> o.headers().add(CONTENT_ENCODING, v))
        .updateIf(
            () -> ofNullable(s3Response.contentLanguage()),
            (o, v) -> o.headers().add(CONTENT_LANGUAGE, v))
        .updateIf(
            () -> ofNullable(s3Response.contentType()), (o, v) -> o.headers().add(CONTENT_TYPE, v))
        .updateIf(
            () -> ofNullable(s3Response.acceptRanges()),
            (o, v) -> o.headers().add(ACCEPT_RANGES, v))
        .updateIf(
            () -> ofNullable(s3Response.expiresString()), (o, v) -> o.headers().add(EXPIRES, v))
        .build();
  }

  private static HttpResponse setHeaders(
      final HttpResponse httpResponse, final PutObjectResponse s3Response) {
    return Builder.create(() -> httpResponse)
        .updateIf(() -> ofNullable(s3Response.eTag()), (o, v) -> o.headers().add(ETAG, v))
        .build();
  }

  private static HttpResponse setHeaders(final HttpResponse response, final File file) {
    return Builder.create(() -> response)
        .update(r -> r.headers().add(CONTENT_LENGTH, file.length()))
        .update(r -> r.headers().add(CONTENT_TYPE, getContentTypeFromName(file.getName())))
        .update(
            r ->
                r.headers()
                    .add(LAST_MODIFIED, HTTP_DATE_FORMAT.format(ofEpochMilli(file.lastModified()))))
        .build();
  }

  private static HttpResponse setHeaders(
      final HttpResponse response, final Map<String, List<String>> headers) {
    headers.forEach((key, value) -> response.headers().add(key, value));

    return response;
  }

  private static HttpResponseStatus status(final SdkResponse response) {
    return new HttpResponseStatus(
        response.sdkHttpResponse().statusCode(),
        response.sdkHttpResponse().statusText().orElse(""));
  }

  private static String stripMetadata(final String path) {
    final int index = path.lastIndexOf(';');

    return index != -1 && path.substring(index + 1).equals(METADATA)
        ? path.substring(0, index)
        : path;
  }

  private static JsonObject toJson(final Map<String, String> metadata) {
    return metadata.entrySet().stream()
        .map(e -> pair(e.getKey(), toJson(e.getValue())))
        .reduce(createObjectBuilder(), (b, p) -> b.add(p.first, p.second), (b1, b2) -> b1)
        .build();
  }

  private static JsonValue toJson(final String value) {
    return from(value).map(JsonValue.class::cast).orElseGet(() -> createValue(value));
  }

  private static Map<String, String[]> toLowerCase(final Map<String, String[]> m) {
    return map(m.entrySet().stream().map(e -> pair(e.getKey().toLowerCase(), e.getValue())));
  }

  private static Map<String, String> toMetadata(final JsonObject json) {
    return map(json.entrySet().stream().map(e -> pair(e.getKey(), string(e.getValue()))));
  }

  private static String unquote(final String s) {
    return s.substring(s.startsWith("\"") ? 1 : 0, s.endsWith("\"") ? s.length() - 1 : s.length());
  }

  private static CompletionStage<Publisher<ByteBuf>> upload(
      final HttpRequest request,
      final Publisher<ByteBuf> body,
      final HttpResponse response,
      final String bucket,
      final String key) {
    return getBoundary(request.headers().get(CONTENT_TYPE))
        .map(
            boundary ->
                upload(bucket, key, body, boundary)
                    .thenApply(v -> ok(response))
                    .exceptionally(t -> exception(response, t)))
        .orElseGet(() -> completedFuture(badRequest(response)));
  }

  private static CompletionStage<Void> upload(
      final String bucket,
      final String key,
      final Publisher<ByteBuf> requestBody,
      final String boundary) {
    final CompletableFuture<Void> future = new CompletableFuture<>();

    with(requestBody)
        .map(ByteBuf::nioBuffer)
        .map(new MultipartDecoder(boundary))
        .mapAsync(bodyPart -> upload(bucket, key, bodyPart.body(), toLowerCase(bodyPart.headers())))
        .get()
        .subscribe(
            lambdaSubscriber(v -> {}, () -> future.complete(null), future::completeExceptionally));

    return future;
  }

  private static CompletionStage<Boolean> upload(
      final String bucket,
      final String key,
      final Publisher<ByteBuffer> body,
      final Map<String, String[]> headers) {
    return filename(headers)
        .map(
            filename ->
                putObject(
                        bucket,
                        key + (key.isEmpty() || key.endsWith("/") ? "" : "/") + filename,
                        contentType(headers),
                        contentLength(headers),
                        body)
                    .thenApply(r -> true))
        .orElseGet(() -> completedFuture(false));
  }

  private Optional<String> bucket(final String path) {
    return bucketConfig(path, config).flatMap(c -> configValue(c::getString, NAME));
  }

  private Optional<List<String>> bucketRoles(final String path) {
    return bucketConfig(path, config).flatMap(c -> configValue(c::getStringList, ROLES));
  }

  private Optional<List<String>> bucketRoles(final HttpRequest request) {
    return path(request).flatMap(this::bucketRoles);
  }

  public void close() {
    httpServer.close();
  }

  private CompletionStage<Publisher<ByteBuf>> delete(
      final HttpRequest request, final HttpResponse response) {
    return writeRequest(
        request, null, response, (req, bo, resp, bucket, key) -> delete(resp, bucket, key));
  }

  private CompletionStage<Publisher<ByteBuf>> get(
      final HttpRequest request, final HttpResponse response) {
    return isLocal(request)
        ? getLocal(request, response, Server::getLocalFile)
        : request(
            request,
            null,
            response,
            (req, body, resp, bucket, key) -> get(req, resp, bucket, key, config));
  }

  private CompletionStage<Publisher<ByteBuf>> getLocal(
      final HttpRequest request,
      final HttpResponse response,
      final Function<File, Publisher<ByteBuffer>> body) {
    return completedFuture(
        localFile(request)
            .map(file -> ok(setHeaders(response, file), body.apply(file)))
            .orElseGet(() -> notFound(response)));
  }

  private CompletionStage<JsonObject> getMetadata(final HttpRequest request) {
    return path(request)
        .map(Server::stripMetadata)
        .flatMap(path -> bucket(path).map(b -> pair(b, key(path, config))))
        .map(
            pair ->
                headObject(pair.first, pair.second)
                    .thenApply(response -> toJson(response.metadata()))
                    .exceptionally(t -> emptyObject()))
        .orElseGet(() -> completedFuture(emptyObject()));
  }

  private RequestHandler handler() {
    return (request, requestBody, response) ->
        Cases.<HttpRequest, CompletionStage<Publisher<ByteBuf>>>withValue(request)
            .or(req -> req.method().equals(DELETE), req -> delete(req, response))
            .or(req -> req.method().equals(GET), req -> get(req, response))
            .or(req -> req.method().equals(HEAD), req -> head(req, response))
            .or(req -> req.method().equals(POST), req -> post(req, requestBody, response))
            .or(req -> req.method().equals(PUT), req -> put(req, requestBody, response))
            .get()
            .orElseGet(() -> simpleResponse(response, NOT_IMPLEMENTED, empty()));
  }

  private boolean hasAccess(final HttpRequest request) {
    return bucketRoles(request)
        .map(roles -> !intersection(roles, getRoles(request, config)).isEmpty())
        .orElse(true);
  }

  private CompletionStage<Publisher<ByteBuf>> head(
      final HttpRequest request, final HttpResponse response) {
    return isLocal(request)
        ? getLocal(request, response, file -> empty())
        : request(
            request,
            null,
            response,
            (req, body, resp, bucket, key) -> head(req, resp, bucket, key, config));
  }

  private boolean isLocal(final String path) {
    return configValue(config::getStringList, LOCAL_PATHS)
        .map(paths -> paths.stream().anyMatch(path::startsWith))
        .orElse(false);
  }

  private boolean isLocal(final HttpRequest request) {
    return path(request).map(this::isLocal).orElse(false);
  }

  private CompletionStage<Publisher<ByteBuf>> post(
      final HttpRequest request, final Publisher<ByteBuf> body, final HttpResponse response) {
    return ofNullable(request.headers().get(CONTENT_TYPE))
        .filter(type -> type.startsWith(FORM_DATA) || type.equals(JSON))
        .map(
            type ->
                type.equals(JSON)
                    ? setMetadata(request, body, response)
                    : upload(request, body, response))
        .orElseGet(() -> completedFuture(badRequest(response)));
  }

  private CompletionStage<Publisher<ByteBuf>> put(
      final HttpRequest request, final Publisher<ByteBuf> body, final HttpResponse response) {
    return writeRequest(request, body, response, Server::put);
  }

  private CompletionStage<Publisher<ByteBuf>> request(
      final HttpRequest request,
      final Publisher<ByteBuf> requestBody,
      final HttpResponse response,
      final RequestFn fn) {
    return path(request)
        .map(
            path ->
                hasAccess(request)
                    ? bucket(path)
                        .map(
                            bucket ->
                                fn.apply(request, requestBody, response, bucket, key(path, config)))
                        .orElseGet(() -> completedFuture(notFound(response)))
                    : completedFuture(forbidden(response)))
        .orElseGet(() -> completedFuture(badRequest(response)));
  }

  public CompletionStage<Boolean> run() {
    return httpServer.run();
  }

  private CompletionStage<Publisher<ByteBuf>> setMetadata(
      final HttpRequest request, final Publisher<ByteBuf> body, final HttpResponse response) {
    return writeRequest(
        request,
        body,
        response,
        (req, bo, resp, bucket, key) -> setMetadata(bo, resp, bucket, key));
  }

  private CompletionStage<Publisher<ByteBuf>> setMetadata(
      final Publisher<ByteBuf> body,
      final HttpResponse response,
      final String bucket,
      final String key) {
    return readJson(body)
        .thenComposeAsync(
            value ->
                ofNullable(value)
                    .filter(v -> !key.endsWith("/"))
                    .map(Server::toMetadata)
                    .map(
                        metadata ->
                            Util.setMetadata(bucket, key, metadata).thenApply(n -> ok(response)))
                    .orElseGet(() -> completedFuture(badRequest(response))));
  }

  public void start() {
    httpServer.start();
  }

  private CompletionStage<Publisher<ByteBuf>> upload(
      final HttpRequest request, final Publisher<ByteBuf> body, final HttpResponse response) {
    return writeRequest(request, body, response, Server::upload);
  }

  public Server withConfig(final Config config) {
    return new Server(port, config);
  }

  public Server withPort(final int port) {
    return new Server(port, config);
  }

  private CompletionStage<Publisher<ByteBuf>> writeRequest(
      final HttpRequest request,
      final Publisher<ByteBuf> requestBody,
      final HttpResponse response,
      final RequestFn fn) {
    return getMetadata(request)
        .thenComposeAsync(
            metadata ->
                isAllowedToWrite(getRoles(request, config), metadata)
                    ? request(request, requestBody, response, fn)
                    : completedFuture(forbidden(response)));
  }

  @FunctionalInterface
  private interface RequestFn {
    CompletionStage<Publisher<ByteBuf>> apply(
        HttpRequest request,
        Publisher<ByteBuf> requestBody,
        HttpResponse response,
        String bucket,
        String key);
  }
}
