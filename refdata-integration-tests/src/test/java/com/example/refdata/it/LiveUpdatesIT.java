package com.example.refdata.it;
import com.example.refdata.client.RefDataClient;
import com.example.refdata.contract.ChangeCodec;
import com.example.refdata.model.*;
import com.example.refdata.protocol.v1.*;
import com.example.refdata.server.RefDataServer;
import com.example.refdata.testing.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import java.io.*;
import java.net.*;
import java.net.http.*;
import java.time.Duration;
import java.util.List;
import static org.junit.Assert.*;
@Category(IntegrationTest.class)
public class LiveUpdatesIT {
 private static final String TOKEN="integration-test-token";
 private static void post(RefDataServer server, ChangeBatch batch) throws Exception {
  var request=HttpRequest.newBuilder(URI.create("http://127.0.0.1:"+server.adminPort()+"/changes"))
   .header("Authorization","Bearer "+TOKEN).timeout(Duration.ofSeconds(15))
   .POST(HttpRequest.BodyPublishers.ofByteArray(ChangeCodec.encode(batch))).build();
  try(var http=HttpClient.newHttpClient()){assertEquals(204,http.send(request,HttpResponse.BodyHandlers.discarding()).statusCode());}
 }
 private static ChangeBatch update(RefDataStore store,String name) {
  var old=store.country("GB");return new ChangeBatch(store.dataVersion(),store.dataVersion()+1,List.of(new ChangeBatch.Change(EntityKind.COUNTRY,ChangeBatch.Operation.UPDATE,"GB",new com.example.refdata.model.Country(old.code(),old.iso3(),old.numericCode(),name,old.region()))));
 }
 private static void awaitVersion(RefDataClient client,long version) throws Exception {
  long end=System.nanoTime()+Duration.ofSeconds(10).toNanos();while(client.currentStore().dataVersion()<version && System.nanoTime()<end)Thread.sleep(10);
  assertEquals(version,client.currentStore().dataVersion());
 }
 @Test public void databaseCommitUpdatesAllConsumersAndSurvivesRestart() throws Exception {
  try(var db=ItSupport.tinyDb()){
   long version;
   try(var server=ItSupport.startServer(db,"rds.db.url",db.url(),"rds.admin.port","0","rds.admin.token",TOKEN);
       var a=ItSupport.client(ItSupport.endpoint(server)).liveUpdates(true).build();
       var b=ItSupport.client(ItSupport.endpoint(server)).liveUpdates(true).build()) {
    var old=a.load().store();b.load();var batch=update(old,"Live country");post(server,batch);
    version=batch.version();awaitVersion(a,version);awaitVersion(b,version);
    assertEquals("Live country",a.currentStore().country("GB").name());assertNotEquals("Live country",old.country("GB").name());
    ItSupport.assertSameStore(ItSupport.referenceStore(db),a.currentStore());
   }
   try(var server=ItSupport.startServer(db);var client=ItSupport.client(ItSupport.endpoint(server)).build()) {
    assertEquals(version,client.load().store().dataVersion());assertEquals("Live country",client.currentStore().country("GB").name());
   }
  }
 }
 @Test public void updateNeverPrecedesSnapshotEndOnTcp() throws Exception {
  try(var db=ItSupport.tinyDb();var server=ItSupport.startServer(db,"rds.db.url",db.url(),"rds.admin.port","0","rds.admin.token",TOKEN,"rds.tcp.send.buffer","1024");
      var socket=new Socket("127.0.0.1",server.tcpPort())) {
   socket.setSoTimeout(15000);var in=new DataInputStream(socket.getInputStream());var out=new DataOutputStream(socket.getOutputStream());
   send(out,Envelope.newBuilder().setHello(Hello.newBuilder().setProtocolMajor(1).addSchemaMajors(1).setClientName("ordering").setKeepAlive(true).setLiveUpdates(true)).build());
   assertTrue(read(in).getHelloAck().getLiveUpdates());
   send(out,Envelope.newBuilder().setSnapshotRequest(SnapshotRequest.newBuilder().setRequestId(1)).build());
   assertTrue(read(in).hasSnapshotBegin());
   var batch=update(server.currentStore(),"During transfer");post(server,batch);
   boolean ended=false;int chunks=0;
   while(true){var frame=read(in);if(frame.hasSnapshotChunk()){assertFalse(ended);chunks++;}
    if(frame.hasSnapshotEnd())ended=true;
    if(frame.getBodyCase()==Envelope.BodyCase.UPDATE_BATCH){assertTrue(ended);assertTrue(chunks>0);assertEquals(batch,ChangeCodec.decode(frame.getUpdateBatch().toByteArray()));break;}}
  }
 }

 @Test public void allEntityCrudAndRollback() throws Exception {
  try(var db=ItSupport.tinyDb();var server=ItSupport.startServer(db,"rds.db.url",db.url(),"rds.admin.port","0","rds.admin.token",TOKEN);
      var client=ItSupport.client(ItSupport.endpoint(server)).liveUpdates(true).build()) {
   var initial=client.load().store();
   int countryNumber=1;while(initial.countryByNumeric(countryNumber)!=null)countryNumber++;
   int currencyNumber=1;while(initial.currencyByNumeric(currencyNumber)!=null)currencyNumber++;
   Object[] rows={
    copy(initial.countries().get(0),java.util.Map.of("code","ZZ","iso3","ZZZ","numericCode",countryNumber)),
    copy(initial.currencies().get(0),java.util.Map.of("code","ZZZ","numericCode",currencyNumber)),
    copy(initial.clients().get(0),java.util.Map.of("id",9000000L,"ref","LIVE-CLIENT","lei","12345678901234567890","incorporationCountry","ZZ","parentClientId",0L)),
    copy(initial.addresses().get(0),java.util.Map.of("id",9000000L,"clientId",9000000L,"countryCode","ZZ")),
    copy(initial.accounts().get(0),java.util.Map.of("id",9000000L,"number","LIVE-ACCOUNT","clientId",9000000L,"baseCurrency","ZZZ","bookingCountry","ZZ")),
    copy(initial.settlementInstructions().get(0),java.util.Map.of("id",9000000L,"accountId",9000000L,"currencyCode","ZZZ"))};
   var kinds=EntityKind.values();var changes=new java.util.ArrayList<ChangeBatch.Change>();
   for(int i=0;i<6;i++)changes.add(new ChangeBatch.Change(kinds[i],ChangeBatch.Operation.INSERT,i==0?"ZZ":i==1?"ZZZ":"9000000",rows[i]));
   long version=initial.dataVersion();
   // A valid final graph in the wrong SQL dependency order must roll back every earlier write.
   var wrongOrder=List.of(changes.get(0),changes.get(3),changes.get(2));
   var rollbackBatch=new ChangeBatch(version,version+1,wrongOrder);
   try(var http=HttpClient.newHttpClient()) {
    var req=HttpRequest.newBuilder(URI.create("http://127.0.0.1:"+server.adminPort()+"/changes")).header("Authorization","Bearer "+TOKEN)
      .POST(HttpRequest.BodyPublishers.ofByteArray(ChangeCodec.encode(rollbackBatch))).build();
    assertEquals(503,http.send(req,HttpResponse.BodyHandlers.discarding()).statusCode());
   }
   assertNull(ItSupport.referenceStore(db).country("ZZ"));assertEquals(version,server.currentStore().dataVersion());
   post(server,new ChangeBatch(version,++version,changes));awaitVersion(client,version);
   ItSupport.assertSameStore(ItSupport.referenceStore(db),client.currentStore());
   var updates=changes.stream().map(c->new ChangeBatch.Change(c.kind(),ChangeBatch.Operation.UPDATE,c.key(),c.record())).toList();
   post(server,new ChangeBatch(version,++version,updates));awaitVersion(client,version);
   ItSupport.assertSameStore(ItSupport.referenceStore(db),client.currentStore());
   java.util.Collections.reverse(changes);
   var deletes=changes.stream().map(c->new ChangeBatch.Change(c.kind(),ChangeBatch.Operation.DELETE,c.key(),null)).toList();
   post(server,new ChangeBatch(version,++version,deletes));awaitVersion(client,version);
   assertNull(client.currentStore().country("ZZ"));assertNull(client.currentStore().client(9000000));
   ItSupport.assertSameStore(ItSupport.referenceStore(db),client.currentStore());
  }
 }
 private static Object copy(Object record,java.util.Map<String,Object> overrides)throws Exception {
  var fields=record.getClass().getRecordComponents();var args=new Object[fields.length];var types=new Class<?>[fields.length];
  for(int i=0;i<fields.length;i++){types[i]=fields[i].getType();args[i]=overrides.containsKey(fields[i].getName())?overrides.get(fields[i].getName()):fields[i].getAccessor().invoke(record);}
  return record.getClass().getDeclaredConstructor(types).newInstance(args);
 }


 @Test public void liveConsumerReconnectsAfterServerRestart() throws Exception {
  try(var db=ItSupport.tinyDb()) {
   var first=ItSupport.startServer(db);int port=first.tcpPort();
   try(var client=ItSupport.client(ItSupport.endpoint(first)).liveUpdates(true).build()) {
    var old=client.load().store();first.close();
    try(var replacement=ItSupport.startServer(db,"rds.tcp.port",Integer.toString(port),"rds.db.url",db.url(),"rds.admin.port","0","rds.admin.token",TOKEN)) {
     var batch=update(replacement.currentStore(),"After restart");post(replacement,batch);
     awaitVersion(client,batch.version());assertEquals("After restart",client.currentStore().country("GB").name());
     assertNotEquals("After restart",old.country("GB").name());
    }
   }finally{first.close();}
  }
 }

 @Test public void droppedConsumerResumesAndReplaysUpdatesCommittedWhileItWasAway() throws Exception {
  try(var db=ItSupport.tinyDb();var server=ItSupport.startServer(db,"rds.db.url",db.url(),"rds.admin.port","0","rds.admin.token",TOKEN);
      var proxy=new TcpProxy("127.0.0.1",server.tcpPort());
      var client=ItSupport.client(proxy.endpoint()).liveUpdates(true).build()) {
   long version=client.load().store().dataVersion();long transfers=server.metrics().transfersCompleted();
   proxy.refuse(true);proxy.cutConnections();
   for(int i=1;i<=3;i++){var batch=update(server.currentStore(),"While away "+i);post(server,batch);version=batch.version();}
   assertTrue("still holds its last view",client.currentStore().dataVersion()<version);
   proxy.refuse(false);
   awaitVersion(client,version);
   assertEquals("While away 3",client.currentStore().country("GB").name());
   ItSupport.assertSameStore(ItSupport.referenceStore(db),client.currentStore());
   assertEquals("resumed without a snapshot transfer",transfers,server.metrics().transfersCompleted());
   // and the resumed session keeps receiving new publications
   var after=update(server.currentStore(),"After resume");post(server,after);awaitVersion(client,after.version());
  }
 }

 private static void send(DataOutputStream out,Envelope e)throws Exception{byte[] bytes=e.toByteArray();out.writeInt(bytes.length);out.write(bytes);out.flush();}
 private static Envelope read(DataInputStream in)throws Exception{int size=in.readInt();if(size<1||size>1048576)throw new IOException("bad frame");return Envelope.parseFrom(in.readNBytes(size));}
}
