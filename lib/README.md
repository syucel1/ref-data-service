# Third-party jars (offline build)

This desktop cannot reach Maven Central, so every dependency is checked in here and referenced from
`build.gradle` by file name. All were taken from publicly available packages on PyPI / the Gradle distribution:

| Jar | Version | Licence | Taken from |
|---|---|---|---|
| netty-*.jar | 4.2.13.Final | Apache 2.0 | PyPI `pyspark` 4.2.0 (deps/jars) |
| derby*.jar | 10.16.1.1 | Apache 2.0 | PyPI `pyspark` 4.2.0 |
| slf4j-api, log4j-* | 2.0.17 / 2.25.4 | MIT / Apache 2.0 | PyPI `pyspark` 4.2.0 |
| lz4-java, zstd-jni | 1.11.0 / 1.5.7-7 | Apache 2.0 / BSD | PyPI `pyspark` 4.2.0 |
| protobuf-java-3.21.7-extracted.jar | 3.21.7 | BSD-3 | the `com/google/protobuf` classes of `h2o.jar` in PyPI `h2o` 3.46.0.12 (unshaded copy of protobuf-java-util 3.21.7 and its runtime) |
| junit, hamcrest-core | 4.13.2 / 1.3 | EPL 1.0 / BSD | /opt/gradle-8.14.3/lib |

`tools/protoc` is libprotoc 3.21.7 from PyPI `protoc-wheel-0` 21.7 — it matches the protobuf-java runtime version.

When Maven access is available, replace this folder with normal coordinates (and add Hibernate for a
`RefDataSource` implementation over ORM).
