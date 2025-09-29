# S3 Server

With the server you can upload multiple files at the same time with a `multipart/form-data` message. The methods `GET`, `PUT` and `HEAD` are also available for individual objects. Metadata in JSON format can be put on the objects.

## Object Keys

The S3 object keys are derived from the URL path. The first segment is mapped to a bucket through the configuration. The remainder of the path will be the object key. Folders should always end with a slash. A particular object version can be addressed by adding `?versionId=<ID>`.

## Uploads

These are done with `multipart/form-data` messages using the `POST` method. The path in the URL should denote the folder that is used or created. For each body part, the last segment of the `filename` parameter in the `Content-Disposition` header is appended to the folder key.

## Metadata

If you post a JSON object to an S3 object, it will be used as the user-defined metadata for it. With the `GET` method you can obtain the metadata by adding `;metadata` to the end of the URL path.

A special and optional field in the metadata is `_acl`. It should be an object with the optional fields `read` and `write`, both of which should be arrays of strings. The arrays represent roles for read and write access respectively. If the `_acl` field is present, the bearer token (JWT) should have a claim with a role that matches for the desired operation. The field in the token can be configured. Its value should be an array of strings.

When you get metadata, the read-only field `_versions` is added. Its fields are the version IDs you can use in the URL query parameter `versionId` of a `GET` request. The values are objects with the fields `eTag`, `lastModified`, `latest` and `size`.

## Configuration

The configuration is managed by the [Lightbend Config package](https://github.com/lightbend/config). By default it will try to load `conf/application.conf`. An alternative configuration may be loaded by adding `-Dconfig.resource=myconfig.conf`, where the file is also supposed to be in the `conf` directory, or `-Dconfig.file=/conf/myconfig.conf`. If no configuration file is available it will load a default one from the resources. The following entries are available:

|Entry|Mandatory|Description|
|---|---|---|
|buckets|Yes|The fields in this object correspond to the first segment in an URL path.The values are again objects with two fields. The `name` field is mandatory. It refers to an S3 bucket. The optional field `roles` is an array of strings. If it is present, the bearer token should have at least one of these roles to work with the bucket. The field `default` can be used to configure a bucket that is used when the first segment of the URL path doesn't match anything.|
|jwtPublicKey|No|A public key in PEM format. If it is present, the signature of the bearer tokens will be verified. If you don't use it, you should deploy the server behind a gateway that does the verification.|
|jwtRolesField|No|It configures the field that is extracted from the JWT bearer tokens for the roles claim. The default value is `roles`.|

## Building and Running

You can build the tool with `mvn clean package`. This will produce a self-contained JAR-file in the `target` directory with the form `pincette-s3-server-<version>-jar-with-dependencies.jar`. You can launch this JAR with `java -jar`.

## Docker

Docker images can be found at [https://hub.docker.com/repository/docker/wdonne/pincette-s3-server](https://hub.docker.com/repository/docker/wdonne/pincette-s3-server).

## Kubernetes

You can mount the configuration in a `ConfigMap` and `Secret` combination. The `ConfigMap` should be mounted at `/conf/application.conf`. You then include the secret in the configuration from where you have mounted it. See also [https://github.com/lightbend/config/blob/main/HOCON.md#include-syntax](https://github.com/lightbend/config/blob/main/HOCON.md#include-syntax).