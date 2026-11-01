# refdata-db

Derby schema, embedded-database helper and the reproducible test-data generator for the reference data service.
Nothing in here is deployed with the service; the service only needs a database that has this schema.

## What is in the module

| Class | Purpose |
|---|---|
| `RefDataSchema` | DDL for COUNTRY, CURRENCY, CLIENT, ADDRESS, ACCOUNT, SETTLEMENT_INSTRUCTION and REFDATA_RELEASE (design sections 5.1/5.2). `tableStatements()` creates the tables, `constraintStatements()` adds the primary, unique and foreign keys, `createStatements()` is both; `create(conn)`, `dropIfExists(conn)`, `tableExists(conn, name)`. |
| `EmbeddedDerby` | Opens or creates a database on disk (`onDisk(dir)`) or in memory (`inMemory(name)`, fast, dropped on close); `connect()`, `url()`, `close()` shuts it down cleanly. |
| `DataGenerator` | Deterministic generator: `new DataGenerator(profile or clientCount, seed).generate(connection)`. Same seed, same rows. |
| `DataGenMain` | Command line around the generator (below). |
| `BadDataInjector` | Damages a generated database in controlled ways (orphan account, duplicate account number, NULL in a mandatory column, unknown enum code, hierarchy cycle, overlapping default SSIs) so the validator can be exercised end to end. |
| `IsoCountries`, `IsoCurrencies` | The ISO 3166 (249 countries) and ISO 4217 (173 active + 26 withdrawn currencies) lists the generator writes. |

## Generating a database

From the repository root:

```
gradle :refdata-db:dataGen -Pargs="--db data/tiny --profile TINY"
gradle :refdata-db:dataGen -Pargs="--db data/medium --profile MEDIUM"
gradle :refdata-db:dataGen -Pargs="--db data/custom --clients 25000 --seed 7"
```

Options: `--db <dir>` (required; the Derby database directory, created if missing — existing tables are dropped and
rewritten), `--profile TINY|MEDIUM|LARGE|XLARGE`, `--clients N` (overrides the profile's client count),
`--seed S` (default 1). The command prints the row counts and the elapsed time. The JDBC URL of the result is
`jdbc:derby:<dir>`.

## Profiles

| Profile | Clients | Addresses (~2.5/client) | Accounts (~3/client) | Instructions (~1.5/account) | Total rows | Use |
|---|---|---|---|---|---|---|
| TINY | 1,000 | ~2,500 | ~3,000 | ~4,500 | ~11 k | unit tests |
| MEDIUM | 100,000 | ~250,000 | ~300,000 | ~450,000 | ~1.1 M | developer machines, the service's default data |
| LARGE | 1,000,000 | ~2.5 M | ~3 M | ~4.5 M | ~11 M | the reference benchmark |
| XLARGE | 5,000,000 | ~12.5 M | ~15 M | ~22.5 M | ~55 M | stress / headroom |

COUNTRY (249 rows) and CURRENCY (199 rows) are the same in every profile. Counts of the child tables vary a little
with the seed because the per-parent distributions are skewed (most clients have 1–3 addresses and accounts, one in
a thousand has hundreds).

Measured on the development machine (4 cores): MEDIUM in about 71 s — 29 s of inserts and 42 s for the keys and
indexes, which are added after the rows so the inserts run unindexed — about 300 MB on disk.

## Shape of the data (design section 5.3)

* Sequential, dense ids from 1 in every table; `row_version >= 1`; `updated_at` within the three years before the
  generator's fixed "today" (`DataGenerator.DEFAULT_AS_OF`, 2026-10-01, so the rows never drift between runs).
* Clients: 70 % carry a 20-character LEI, about 10 % take part in a parent/child hierarchy up to four levels deep
  (a parent always has a lower id than its children, so there are no cycles), 5 % are closed.
* Every client has a primary REGISTERED address; primary addresses of one type never overlap in time.
* Accounts: IBAN-like unique account numbers, about 5 % closed with a `closed_date`; base currencies and booking
  countries are drawn from a realistic "major markets" set most of the time.
* Settlement instructions: 0–4 per account; default instructions of one (account, currency, asset class) form a
  chain of non-overlapping effective periods; BICs are 11 characters.
* One REFDATA_RELEASE row: `release_id = 1`, status PUBLISHED.

## Timestamps and dates

`updated_at` is a `TIMESTAMP` written and read with a UTC calendar (Derby has no time-zone aware type); `DATE`
columns are plain calendar dates. The JDBC loader (`refdata-loader-jdbc`) reads them the same way, so the epoch
values in the model do not depend on the JVM's time zone.
