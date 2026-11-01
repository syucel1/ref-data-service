# refdata-loader-jdbc

`JdbcRefDataSource` implements `com.example.refdata.model.RefDataSource` over plain JDBC (the place a Hibernate
loader would go). Construct it with a JDBC URL or a `Supplier<Connection>` plus a `JdbcLoadConfig` (fetch size
10,000, parallelism 4, read-only by default).

* `preflight()` – `COUNT(*)` per table, the latest PUBLISHED `release_id` from REFDATA_RELEASE (0 when the table is
  empty or missing) and a source description without credentials.
* `load(sink)` – streams the six tables in primary-key order, one thread and one connection per table (bounded by
  the parallelism), each in a read-only REPEATABLE READ transaction with the configured fetch size. Rows are mapped
  to the model records as they arrive: enum codes via `fromDb` (null when unknown, the validator reports it), NULL
  end dates to the open-ended sentinels, DATE to epoch day and TIMESTAMP to epoch milliseconds, both read as UTC.
  Repeated low-cardinality strings (country and currency codes, cities, regions, BICs) are de-duplicated per table
  (capped cache, 100k distinct values per table) so the model holds one instance per distinct value. After all tables
  the release id is read again; if it changed, `JdbcLoadException` is thrown and the caller retries. Returns the
  release id.

## Sink contract

The loader calls the sink from several threads, but in a disciplined way: each table has its own thread, so
*different* sink methods (`client`, `address`, ...) run concurrently with each other, while no single method is ever
invoked from two threads at the same time, and nothing is called after `load` returns. `RefDataStoreBuilder` is
written to exactly this contract; any other sink must be too.

## Tests

`gradle :refdata-loader-jdbc:test` loads a generated TINY database (from `refdata-db`) and compares it with SQL;
`gradle :refdata-loader-jdbc:integrationTest` loads `data/medium` end to end when it exists.
