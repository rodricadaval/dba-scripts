var iterate = db.adminCommand({currentOp: true,"$all": true})

iterate.forEach(printjson);
