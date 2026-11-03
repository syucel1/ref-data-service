package com.example.refdata.contract;
import com.example.refdata.model.*;
import java.io.*;
import java.util.*;
/** Bounded binary transaction envelope containing protobuf entity after-images. */
public final class ChangeCodec {
 private ChangeCodec() {}
 public static byte[] encode(ChangeBatch batch) throws IOException {
  var bytes=new ByteArrayOutputStream();var out=new DataOutputStream(bytes);
  out.writeLong(batch.baseVersion());out.writeLong(batch.version());out.writeInt(batch.changes().size());
  for(var c:batch.changes()) {out.writeInt(c.kind().wireCode());out.writeInt(c.operation().ordinal());out.writeUTF(c.key());byte[] row=c.record()==null?new byte[0]:encodeRow(c.kind(),c.record());out.writeInt(row.length);out.write(row);if(bytes.size()>512*1024)throw new IOException("batch exceeds 512 KiB");}return bytes.toByteArray();
 }
 public static ChangeBatch decode(byte[] bytes) throws IOException {
  if(bytes.length>512*1024)throw new IOException("batch exceeds 512 KiB");var in=new DataInputStream(new ByteArrayInputStream(bytes));long base=in.readLong(),version=in.readLong();int count=in.readInt();if(count<1||count>1000)throw new IOException("invalid count");var changes=new ArrayList<ChangeBatch.Change>();
  try {for(int i=0;i<count;i++){var kind=EntityKind.fromWire(in.readInt());int op=in.readInt();if(kind==null||op<0||op>2)throw new IOException("invalid operation");String key=in.readUTF();int length=in.readInt();if(length<0||length>in.available())throw new IOException("invalid length");byte[] row=in.readNBytes(length);changes.add(new ChangeBatch.Change(kind,ChangeBatch.Operation.values()[op],key,length==0?null:decodeRow(kind,row)));}if(in.available()!=0)throw new IOException("trailing bytes");return new ChangeBatch(base,version,changes);}catch(IllegalArgumentException e){throw new IOException("invalid batch",e);}
 }
 public static byte[] encodeRow(EntityKind kind,Object row) {return switch(kind) {case COUNTRY -> EntityCodec.toProto((com.example.refdata.model.Country)row).toByteArray();case CURRENCY -> EntityCodec.toProto((com.example.refdata.model.Currency)row).toByteArray();case CLIENT -> EntityCodec.toProto((com.example.refdata.model.Client)row).toByteArray();case ADDRESS -> EntityCodec.toProto((com.example.refdata.model.Address)row).toByteArray();case ACCOUNT -> EntityCodec.toProto((com.example.refdata.model.Account)row).toByteArray();case SETTLEMENT_INSTRUCTION -> EntityCodec.toProto((com.example.refdata.model.SettlementInstruction)row).toByteArray();};} public static Object decodeRow(EntityKind kind,byte[] row) throws IOException {return switch(kind){case COUNTRY -> EntityCodec.fromProto(com.example.refdata.schema.v1.Country.parseFrom(row));case CURRENCY -> EntityCodec.fromProto(com.example.refdata.schema.v1.Currency.parseFrom(row));case CLIENT -> EntityCodec.fromProto(com.example.refdata.schema.v1.Client.parseFrom(row));case ADDRESS -> EntityCodec.fromProto(com.example.refdata.schema.v1.Address.parseFrom(row));case ACCOUNT -> EntityCodec.fromProto(com.example.refdata.schema.v1.Account.parseFrom(row));case SETTLEMENT_INSTRUCTION -> EntityCodec.fromProto(com.example.refdata.schema.v1.SettlementInstruction.parseFrom(row));};}}