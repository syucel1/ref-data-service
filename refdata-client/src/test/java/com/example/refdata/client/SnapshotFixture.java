package com.example.refdata.client;

import com.example.refdata.contract.Digests;
import com.example.refdata.contract.EntityCodec;
import com.example.refdata.contract.Framing;
import com.example.refdata.model.Account;
import com.example.refdata.model.AccountType;
import com.example.refdata.model.Address;
import com.example.refdata.model.AddressType;
import com.example.refdata.model.AssetClass;
import com.example.refdata.model.Client;
import com.example.refdata.model.ClientType;
import com.example.refdata.model.Country;
import com.example.refdata.model.Currency;
import com.example.refdata.model.EntityKind;
import com.example.refdata.model.SettlementInstruction;
import com.example.refdata.model.Status;
import com.example.refdata.protocol.v1.Codec;
import com.example.refdata.protocol.v1.EntityManifest;
import com.example.refdata.protocol.v1.SnapshotChunk;
import com.example.refdata.protocol.v1.SnapshotEnd;
import com.example.refdata.protocol.v1.SnapshotInfo;
import com.example.refdata.schema.v1.AccountBatch;
import com.example.refdata.schema.v1.AddressBatch;
import com.example.refdata.schema.v1.ClientBatch;
import com.example.refdata.schema.v1.CountryBatch;
import com.example.refdata.schema.v1.CurrencyBatch;
import com.example.refdata.schema.v1.SettlementInstructionBatch;
import com.github.luben.zstd.Zstd;
import com.google.protobuf.ByteString;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;

import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * A snapshot encoded exactly as the implementation notes prescribe (one entity type per chunk, primary-key order,
 * types in {@link EntityKind} order, CRC over the payload as sent, SHA-256 over the uncompressed payloads, content
 * hash and snapshot id from {@link Digests}), built from hand-made records. Used by {@link FakeServer} scripts.
 */
public final class SnapshotFixture {
    /** An entity type number this build does not know, for forward-compatibility tests. */
    public static final int UNKNOWN_ENTITY_TYPE = 99;

    private final SnapshotInfo info;
    private final List<SnapshotChunk> chunks;
    private final Codec codec;

    private SnapshotFixture(SnapshotInfo info, List<SnapshotChunk> chunks, Codec codec) {
        this.info = info;
        this.chunks = chunks;
        this.codec = codec;
    }

    public static Builder builder() { return new Builder(); }

    /** A handful of consistent records of every kind, two records per chunk: 10 chunks. */
    public static SnapshotFixture sample() { return sampleBuilder().build(); }

    /** Like {@link #sample()} with one more country, so the content (and the snapshot id) differs. */
    public static SnapshotFixture sampleVariant(long dataVersion) {
        return sampleBuilder().dataVersion(dataVersion).country(new Country("DE", "DEU", 276, "Germany", "EMEA")).build();
    }

    public static Builder sampleBuilder() {
        Builder b = builder().recordsPerChunk(2).dataVersion(7);
        b.country(new Country("GB", "GBR", 826, "United Kingdom", "EMEA"));
        b.country(new Country("US", "USA", 840, "United States", "AMER"));
        b.currency(new Currency("GBP", 826, "Pound sterling", 2, true));
        b.currency(new Currency("USD", 840, "US dollar", 2, true));
        b.client(new Client(1L, "C-0001", "5493001KJTIIGC8Y1R12", "Alpha Holdings plc", "Alpha", ClientType.CORPORATE, Status.ACTIVE, "GB", Client.NO_PARENT, 3, 18000, 1, 1_700_000_000_000L));
        b.client(new Client(2L, "C-0002", null, "Beta Fund LP", null, ClientType.FUND, Status.ACTIVE, "US", Client.NO_PARENT, 5, 18100, 1, 1_700_000_000_000L));
        b.client(new Client(3L, "C-0003", null, "Alpha Capital Ltd", "AlphaCap", ClientType.CORPORATE, Status.SUSPENDED, "GB", 1L, 4, 18200, 2, 1_700_000_000_000L));
        b.address(new Address(10L, 1L, AddressType.REGISTERED, "1 Main St", null, null, "London", null, "EC1A 1AA", "GB", true, 18000, Address.OPEN_ENDED, 1, 1_700_000_000_000L));
        b.address(new Address(11L, 2L, AddressType.REGISTERED, "2 Wall St", "Floor 3", null, "New York", "NY", "10005", "US", true, 18100, Address.OPEN_ENDED, 1, 1_700_000_000_000L));
        b.address(new Address(12L, 3L, AddressType.MAILING, "3 High St", null, null, "Leeds", null, "LS1 1AA", "GB", false, 18200, 19000, 1, 1_700_000_000_000L));
        b.account(new Account(100L, "ACC-100", 1L, AccountType.CASH, "GBP", "GB", Status.ACTIVE, 18001, Account.NOT_CLOSED, 1, 1_700_000_000_000L));
        b.account(new Account(101L, "ACC-101", 2L, AccountType.CUSTODY, "USD", "US", Status.ACTIVE, 18101, Account.NOT_CLOSED, 1, 1_700_000_000_000L));
        b.account(new Account(102L, "ACC-102", 3L, AccountType.TRADING, "GBP", "GB", Status.CLOSED, 18201, 18900, 1, 1_700_000_000_000L));
        b.settlementInstruction(new SettlementInstruction(1000L, 100L, "GBP", AssetClass.CASH, "BARCGB22", "BARCGB22", "12345678", true, 18001, SettlementInstruction.OPEN_ENDED, 1, 1_700_000_000_000L));
        b.settlementInstruction(new SettlementInstruction(1001L, 101L, "USD", AssetClass.EQUITY, "CHASUS33", "CHASUS33", "87654321", true, 18101, SettlementInstruction.OPEN_ENDED, 1, 1_700_000_000_000L));
        b.settlementInstruction(new SettlementInstruction(1002L, 102L, "GBP", AssetClass.FIXED_INCOME, "BARCGB22", "HSBCGB2L", "11112222", false, 18201, SettlementInstruction.OPEN_ENDED, 1, 1_700_000_000_000L));
        return b;
    }

    public SnapshotInfo info() { return info; }
    public Codec codec() { return codec; }
    public byte[] snapshotId() { return info.getSnapshotId().toByteArray(); }
    /** All chunks in seq order. */
    public List<SnapshotChunk> chunks() { return chunks; }
    /** The entity type numbers in the manifest, in stream order. */
    public List<Integer> entityTypes() { return info.getEntitiesList().stream().map(EntityManifest::getEntityType).toList(); }

    /** The chunks of the given types (empty = all), in seq order. */
    public List<SnapshotChunk> chunksFor(Collection<Integer> types) {
        if (types.isEmpty()) return chunks;
        return chunks.stream().filter(c -> types.contains(c.getEntityType())).toList();
    }

    /** The SnapshotEnd for a transfer of the given types (empty = all), totals over the whole snapshot as 9.8 requires. */
    public SnapshotEnd end(long requestId, Collection<Integer> types) {
        List<SnapshotChunk> sent = chunksFor(types);
        long records = 0;
        for (SnapshotChunk c : sent) records += c.getRecordCount();
        return SnapshotEnd.newBuilder().setRequestId(requestId).setChunksSent(sent.size()).setRecordsSent(records).setContentSha256(info.getContentSha256()).build();
    }

    /** A copy whose manifest hash for one type is wrong (to provoke a per-entity hash failure). */
    public SnapshotFixture withCorruptEntityHash(EntityKind kind) {
        SnapshotInfo.Builder b = info.toBuilder();
        for (int i = 0; i < b.getEntitiesCount(); i++) {
            if (b.getEntities(i).getEntityType() == kind.wireCode()) {
                byte[] h = b.getEntities(i).getEntitySha256().toByteArray();
                h[0] ^= 0x55;
                b.setEntities(i, b.getEntities(i).toBuilder().setEntitySha256(ByteString.copyFrom(h)));
            }
        }
        return new SnapshotFixture(b.build(), chunks, codec);
    }

    /** Collects records and encodes them. */
    public static final class Builder {
        private final List<Country> countries = new ArrayList<>();
        private final List<Currency> currencies = new ArrayList<>();
        private final List<Client> clients = new ArrayList<>();
        private final List<Address> addresses = new ArrayList<>();
        private final List<Account> accounts = new ArrayList<>();
        private final List<SettlementInstruction> ssis = new ArrayList<>();
        private final List<byte[]> unknownTypePayloads = new ArrayList<>();
        private int recordsPerChunk = 1000;
        private Codec codec = Codec.CODEC_NONE;
        private long dataVersion = 1;

        public Builder country(Country c) { countries.add(c); return this; }
        public Builder currency(Currency c) { currencies.add(c); return this; }
        public Builder client(Client c) { clients.add(c); return this; }
        public Builder address(Address a) { addresses.add(a); return this; }
        public Builder account(Account a) { accounts.add(a); return this; }
        public Builder settlementInstruction(SettlementInstruction s) { ssis.add(s); return this; }
        public Builder recordsPerChunk(int n) { this.recordsPerChunk = n; return this; }
        public Builder codec(Codec c) { this.codec = c; return this; }
        public Builder dataVersion(long v) { this.dataVersion = v; return this; }

        /** Adds chunks of {@link #UNKNOWN_ENTITY_TYPE} after the known types; each payload counts as one record. */
        public Builder unknownTypeChunk(byte[] payload) { unknownTypePayloads.add(payload); return this; }

        public SnapshotFixture build() {
            List<SnapshotChunk> chunks = new ArrayList<>();
            List<EntityManifest> manifest = new ArrayList<>();
            List<byte[]> entityHashes = new ArrayList<>();
            long totalRecords = 0, totalBytes = 0;
            Map<Integer, List<byte[]>> payloadsByType = new LinkedHashMap<>();
            payloadsByType.put(EntityKind.COUNTRY.wireCode(), batches(countries, Comparator.comparing(Country::code),
                    l -> CountryBatch.newBuilder().addAllItems(l.stream().map(EntityCodec::toProto).toList()).build().toByteArray()));
            payloadsByType.put(EntityKind.CURRENCY.wireCode(), batches(currencies, Comparator.comparing(Currency::code),
                    l -> CurrencyBatch.newBuilder().addAllItems(l.stream().map(EntityCodec::toProto).toList()).build().toByteArray()));
            payloadsByType.put(EntityKind.CLIENT.wireCode(), batches(clients, Comparator.comparingLong(Client::id),
                    l -> ClientBatch.newBuilder().addAllItems(l.stream().map(EntityCodec::toProto).toList()).build().toByteArray()));
            payloadsByType.put(EntityKind.ADDRESS.wireCode(), batches(addresses, Comparator.comparingLong(Address::id),
                    l -> AddressBatch.newBuilder().addAllItems(l.stream().map(EntityCodec::toProto).toList()).build().toByteArray()));
            payloadsByType.put(EntityKind.ACCOUNT.wireCode(), batches(accounts, Comparator.comparingLong(Account::id),
                    l -> AccountBatch.newBuilder().addAllItems(l.stream().map(EntityCodec::toProto).toList()).build().toByteArray()));
            payloadsByType.put(EntityKind.SETTLEMENT_INSTRUCTION.wireCode(), batches(ssis, Comparator.comparingLong(SettlementInstruction::id),
                    l -> SettlementInstructionBatch.newBuilder().addAllItems(l.stream().map(EntityCodec::toProto).toList()).build().toByteArray()));
            if (!unknownTypePayloads.isEmpty()) payloadsByType.put(UNKNOWN_ENTITY_TYPE, unknownTypePayloads);

            List<Integer> chunkTypes = new ArrayList<>(), chunkCounts = new ArrayList<>();
            for (Map.Entry<Integer, List<byte[]>> e : payloadsByType.entrySet()) {
                int type = e.getKey();
                MessageDigest sha = Digests.sha256();
                int firstSeq = chunks.size();
                long records = 0, bytes = 0;
                for (byte[] uncompressed : e.getValue()) {
                    int count = type == UNKNOWN_ENTITY_TYPE ? 1 : recordCount(type, uncompressed);
                    byte[] payload = compress(codec, uncompressed);
                    chunks.add(SnapshotChunk.newBuilder().setSeq(chunks.size()).setEntityType(type).setRecordCount(count).setCodec(codec)
                            .setUncompressedSize(uncompressed.length).setCrc32(Digests.crc32(payload)).setPayload(ByteString.copyFrom(payload)).build());
                    sha.update(uncompressed);
                    records += count;
                    bytes += uncompressed.length;
                    chunkTypes.add(type);
                    chunkCounts.add(count);
                }
                byte[] hash = sha.digest();
                entityHashes.add(hash);
                manifest.add(EntityManifest.newBuilder().setEntityType(type).setRecordCount(records).setFirstSeq(firstSeq)
                        .setChunkCount(chunks.size() - firstSeq).setUncompressedBytes(bytes).setEntitySha256(ByteString.copyFrom(hash)).build());
                totalRecords += records;
                totalBytes += bytes;
            }
            byte[] content = Digests.contentSha256(entityHashes);
            byte[] id = Digests.snapshotId(Framing.SCHEMA_MAJOR, Framing.SCHEMA_MINOR, chunkTypes.stream().mapToInt(Integer::intValue).toArray(),
                    chunkCounts.stream().mapToInt(Integer::intValue).toArray(), content);
            SnapshotInfo info = SnapshotInfo.newBuilder().setSnapshotId(ByteString.copyFrom(id)).setDataVersion(dataVersion)
                    .setSchemaMajor(Framing.SCHEMA_MAJOR).setSchemaMinor(Framing.SCHEMA_MINOR).setBuiltAtEpochMs(1_700_000_000_000L)
                    .setSource("fixture").setContentSha256(ByteString.copyFrom(content)).addAllEntities(manifest)
                    .setTotalChunks(chunks.size()).setTotalRecords(totalRecords).setTotalUncompressedBytes(totalBytes).build();
            return new SnapshotFixture(info, List.copyOf(chunks), codec);
        }

        private <T> List<byte[]> batches(List<T> rows, Comparator<T> order, Function<List<T>, byte[]> encode) {
            List<T> sorted = new ArrayList<>(rows);
            sorted.sort(order);
            List<byte[]> out = new ArrayList<>();
            for (int i = 0; i < sorted.size(); i += recordsPerChunk) {
                out.add(encode.apply(sorted.subList(i, Math.min(sorted.size(), i + recordsPerChunk))));
            }
            return out;
        }

        private static int recordCount(int type, byte[] batch) {
            try {
                return switch (EntityKind.fromWire(type)) {
                    case COUNTRY -> CountryBatch.parseFrom(batch).getItemsCount();
                    case CURRENCY -> CurrencyBatch.parseFrom(batch).getItemsCount();
                    case CLIENT -> ClientBatch.parseFrom(batch).getItemsCount();
                    case ADDRESS -> AddressBatch.parseFrom(batch).getItemsCount();
                    case ACCOUNT -> AccountBatch.parseFrom(batch).getItemsCount();
                    case SETTLEMENT_INSTRUCTION -> SettlementInstructionBatch.parseFrom(batch).getItemsCount();
                };
            } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                throw new IllegalStateException(e);
            }
        }

        private static byte[] compress(Codec codec, byte[] data) {
            return switch (codec) {
                case CODEC_LZ4 -> {
                    LZ4Compressor c = LZ4Factory.fastestInstance().fastCompressor();
                    byte[] buf = new byte[c.maxCompressedLength(data.length)];
                    int n = c.compress(data, 0, data.length, buf, 0, buf.length);
                    yield java.util.Arrays.copyOf(buf, n);
                }
                case CODEC_ZSTD -> Zstd.compress(data);
                default -> data;
            };
        }
    }
}
