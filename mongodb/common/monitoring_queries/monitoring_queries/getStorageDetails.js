function getDetailedInformation() {
 var db = this.db.getSiblingDB("admin");
 var totalIndexSize = 0,
   totalStorageSize = 0,
   totalDataSize = 0,
   formatSize = 1024 * 1024 * 1024;
 var dbs = db.runCommand({ listDatabases: 1 }).databases;
 dbs.forEach(function (database) {
   if (!/admin|Cnfig|local/.test(database.name)) {
     db = db.getSiblingDB(database.name);
     stats = db.stats();
     print(`\nDB: ${database.name}`);
     stats.collections && print(` Collections: ${stats.collections}`);
     stats.views && print(` Views: ${stats.views}`);
     print(`  # of Indexes: ${stats.indexes}`);
     print(`  Index Size: ${(stats.indexSize / formatSize).toFixed(2)}GB`);
     print(`  Data Size: ${(stats.dataSize / formatSize).toFixed(2)}GB`);
     print(`  Storage Size: ${(stats.storageSize / formatSize).toFixed(2)}GB`);
     print(`  Average Obj Size: ${stats.avgObjSize.toFixed(0)}B`);
     totalDataSize += stats.dataSize;
     totalStorageSize += stats.storageSize;
     totalIndexSize += stats.indexSize;
   }
 });
 print("\nTotal:");
 print(`  Data Size: ${(totalDataSize / formatSize).toFixed(2)}GB`);
 print(`  Storage Size: ${(totalStorageSize / formatSize).toFixed(2)}GB`);
 print(`  Index Size: ${(totalIndexSize / formatSize).toFixed(2)}GB`);
 
 if (!db.serverStatus().errmsg) {
   print("\nAdditional Info:");
   print(`  MongoDB version: ${db.serverStatus().version}`);
   db.serverStatus().storageEngine &&
   print(`  Storage Engine: ${db.serverStatus().storageEngine.name}`);
   print(`  CPU Cores: ${db.hostInfo().system.numCores}`);
   print(`  Memory Size(MB): ${db.hostInfo().system.memSizeMB}`);
 }
}
 
getDetailedInformation();
