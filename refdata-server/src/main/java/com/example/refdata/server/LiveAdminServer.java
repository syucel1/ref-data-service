package com.example.refdata.server;
import com.example.refdata.model.*;
import com.example.refdata.contract.ChangeCodec;
import com.sun.net.httpserver.*;
import java.sql.*;
import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

/** Service-owned transactional admin writer for the embedded database. */
public final class LiveAdminServer implements AutoCloseable {
 private final RefDataServer owner; private final String url, token;
 private final HttpServer http;
 private final ThreadPoolExecutor workers = new ThreadPoolExecutor(1,1,0,TimeUnit.SECONDS,new ArrayBlockingQueue<>(16),r->{Thread t=new Thread(r,"rds-admin");t.setDaemon(true);return t;},new ThreadPoolExecutor.AbortPolicy());
 private final ScheduledExecutorService poller=Executors.newSingleThreadScheduledExecutor(r->{Thread t=new Thread(r,"rds-outbox");t.setDaemon(true);return t;});
 private volatile Throwable failure;
 public LiveAdminServer(RefDataServer owner,String url,int port,String token) throws IOException,SQLException {
  if(token==null||token.length()<16)throw new IllegalArgumentException("admin token must contain at least 16 characters");
  this.owner=owner;this.url=url;this.token=token;
  try(Connection c=connect();Statement s=c.createStatement()) {
   try {s.executeUpdate("CREATE TABLE REFDATA_OUTBOX (version BIGINT PRIMARY KEY, payload BLOB(524288) NOT NULL)");}
   catch(SQLException e){if(!"X0Y32".equals(e.getSQLState()))throw e;}
  }
  http=HttpServer.create(new InetSocketAddress("127.0.0.1",port),16);http.setExecutor(workers);http.createContext("/changes",this::handle);http.start();
  poller.scheduleWithFixedDelay(()->{try{drain();failure=null;}catch(Exception e){failure=e;}},0,100,TimeUnit.MILLISECONDS);
 }
 private Connection connect() throws SQLException {return DriverManager.getConnection(url,owner.config().dbUser,owner.config().dbPassword());}
 public int port(){return http.getAddress().getPort();}
 private void handle(HttpExchange ex) throws IOException {
  try(ex){
   if(!("Bearer "+token).equals(ex.getRequestHeaders().getFirst("Authorization"))){ex.sendResponseHeaders(401,-1);return;}
   if(!ex.getRequestURI().getPath().equals("/changes")){ex.sendResponseHeaders(404,-1);return;}
   if(!ex.getRequestMethod().equals("POST")){ex.sendResponseHeaders(405,-1);return;}
   byte[] bytes=ex.getRequestBody().readNBytes(524289);if(bytes.length>524288){ex.sendResponseHeaders(413,-1);return;}
   try{commit(ChangeCodec.decode(bytes));ex.sendResponseHeaders(204,-1);}
   catch(IllegalArgumentException e){ex.sendResponseHeaders(409,-1);}
   catch(Exception e){ex.sendResponseHeaders(503,-1);}
  }
 }
 public synchronized void commit(ChangeBatch batch) throws SQLException,IOException {
  drain(); failure=null;
  batch.apply(owner.currentStore()); // validate the complete resulting graph before committing
  byte[] payload=ChangeCodec.encode(batch);
  if(payload.length+32>owner.config().frameMaxBytes)throw new IOException("batch exceeds configured frame size");
  try(Connection c=connect()){
   c.setAutoCommit(false);
   try{
    for(var change:batch.changes()) write(c,change);
    try(var s=c.prepareStatement("INSERT INTO REFDATA_OUTBOX(version,payload) VALUES (?,?)")){s.setLong(1,batch.version());s.setBytes(2,payload);s.executeUpdate();}
    try(var s=c.prepareStatement("INSERT INTO REFDATA_RELEASE(release_id,status,published_at,description) VALUES (?,'PUBLISHED',CURRENT_TIMESTAMP,'live transaction')")){s.setLong(1,batch.version());s.executeUpdate();}
    c.commit();
   }catch(Exception e){c.rollback();if(e instanceof SQLException sql)throw sql;if(e instanceof IOException io)throw io;throw new IOException(e);}
  }
  drain();
 }
 private synchronized void drain() throws SQLException,IOException {
  try(Connection c=connect();var s=c.prepareStatement("SELECT payload FROM REFDATA_OUTBOX WHERE version > ? ORDER BY version")){
   s.setLong(1,owner.currentStore().dataVersion());s.setMaxRows(32);
   try(var rs=s.executeQuery()){while(rs.next())owner.apply(ChangeCodec.decode(rs.getBytes(1)));}
   try(var cleanup=c.prepareStatement("DELETE FROM REFDATA_OUTBOX WHERE version < ?")){cleanup.setLong(1,owner.currentStore().dataVersion()-32);cleanup.executeUpdate();}
  }
 }
 private static void write(Connection c,ChangeBatch.Change change) throws Exception {
  String[][] fields=fields(change.kind());String table=change.kind().tableName();String key=fields[0][0];
  if(change.operation()==ChangeBatch.Operation.DELETE){try(var s=c.prepareStatement("DELETE FROM "+table+" WHERE "+key+"=?")){s.setObject(1,keyValue(change));if(s.executeUpdate()!=1)throw new SQLException("missing delete key");}return;}
  String sql;
  if(change.operation()==ChangeBatch.Operation.INSERT)sql="INSERT INTO "+table+" ("+String.join(",",Arrays.stream(fields).map(f->f[0]).toList())+") VALUES ("+String.join(",",Collections.nCopies(fields.length,"?"))+")";
  else sql="UPDATE "+table+" SET "+String.join(",",Arrays.stream(fields).skip(1).map(f->f[0]+"=?").toList())+" WHERE "+key+"=?";
  try(var s=c.prepareStatement(sql)){int index=1;for(int i=change.operation()==ChangeBatch.Operation.INSERT?0:1;i<fields.length;i++){
   Object v=change.record().getClass().getMethod(fields[i][1]).invoke(change.record());String column=fields[i][0];
   if(v instanceof Enum<?> e)v=e.name();
   if(column.equals("parent_client_id")&&Long.valueOf(0).equals(v))v=null;
   if(column.endsWith("_date")||column.equals("valid_from")||column.equals("valid_to")||column.equals("effective_from")||column.equals("effective_to")){
    int day=((Number)v).intValue();v=day==Integer.MAX_VALUE||day==Integer.MIN_VALUE?null:java.sql.Date.valueOf(java.time.LocalDate.ofEpochDay(day));
   }
   if(column.equals("updated_at"))v=new Timestamp(((Number)v).longValue());
   if(v==null&&(column.equals("short_name")||change.kind()==EntityKind.COUNTRY&&column.equals("region")))v="";
   if(v instanceof Timestamp timestamp) s.setTimestamp(index++,timestamp,java.util.Calendar.getInstance(java.util.TimeZone.getTimeZone("UTC")));
   else s.setObject(index++,v);
  }if(change.operation()==ChangeBatch.Operation.UPDATE)s.setObject(index,keyValue(change));if(s.executeUpdate()!=1)throw new SQLException("missing update key");}
 }
 private static Object keyValue(ChangeBatch.Change c){return c.kind()==EntityKind.COUNTRY||c.kind()==EntityKind.CURRENCY?c.key():Long.valueOf(c.key());}
 private static String[][] fields(EntityKind kind){return switch(kind){
case COUNTRY -> new String[][]{{"country_code","code"},{"iso3","iso3"},{"numeric_code","numericCode"},{"name","name"},{"region","region"}};
case CURRENCY -> new String[][]{{"currency_code","code"},{"numeric_code","numericCode"},{"name","name"},{"minor_units","minorUnits"},{"active","active"}};
case CLIENT -> new String[][]{{"client_id","id"},{"client_ref","ref"},{"lei","lei"},{"legal_name","legalName"},{"short_name","shortName"},{"client_type","type"},{"status","status"},{"incorporation_country","incorporationCountry"},{"parent_client_id","parentClientId"},{"risk_rating","riskRating"},{"onboarded_date","onboardedDate"},{"row_version","rowVersion"},{"updated_at","updatedAt"}};
case ADDRESS -> new String[][]{{"address_id","id"},{"client_id","clientId"},{"address_type","type"},{"line1","line1"},{"line2","line2"},{"line3","line3"},{"city","city"},{"region","region"},{"postal_code","postalCode"},{"country_code","countryCode"},{"is_primary","primary"},{"valid_from","validFrom"},{"valid_to","validTo"},{"row_version","rowVersion"},{"updated_at","updatedAt"}};
case ACCOUNT -> new String[][]{{"account_id","id"},{"account_number","number"},{"client_id","clientId"},{"account_type","type"},{"base_currency","baseCurrency"},{"booking_country","bookingCountry"},{"status","status"},{"opened_date","openedDate"},{"closed_date","closedDate"},{"row_version","rowVersion"},{"updated_at","updatedAt"}};
case SETTLEMENT_INSTRUCTION -> new String[][]{{"ssi_id","id"},{"account_id","accountId"},{"currency_code","currencyCode"},{"asset_class","assetClass"},{"beneficiary_bic","beneficiaryBic"},{"agent_bic","agentBic"},{"agent_account","agentAccount"},{"is_default","isDefault"},{"effective_from","effectiveFrom"},{"effective_to","effectiveTo"},{"row_version","rowVersion"},{"updated_at","updatedAt"}};
};}
 public void close(){http.stop(0);workers.shutdownNow();poller.shutdownNow();try{workers.awaitTermination(5,TimeUnit.SECONDS);poller.awaitTermination(5,TimeUnit.SECONDS);}catch(InterruptedException e){Thread.currentThread().interrupt();}}
}