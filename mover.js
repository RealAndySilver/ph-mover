//const express = require('express');
//const server = require('http').Server(app);
var http = require('http');
const port = process.env.PORT || 3002;
var MongoClient = require('mongodb').MongoClient;
var fs = require('fs');
var url = 'mongodb://localhost:27017/test';
var time_to_move_data = true;
var first_time_moving_data = true;

setInterval(function(){
	var date = new Date();
	if(date.getMinutes()==2 || first_time_moving_data){
		if(!time_to_move_data){
			return;
		}
		move();
		time_to_move_data = false;
		first_time_moving_data = false;
	}
	else{
		time_to_move_data = true;
	}
}, 1000);

function move(){
	MongoClient.connect(url, (err, db) => {
		if(err){
			move();
			return;
		}
		var date = new Date();
		var bulkInsert = db.collection('past').initializeUnorderedBulkOp()
		//var bulkRemove = db.collection('current').initializeUnorderedBulkOp()
		var x = 1000
		var counter = 0
		
		var start = null;
		var finish = null;
		start = new Date();
		
		date.setMonth(date.getMonth());
		date.setHours(date.getHours()-1);
	    date.setMinutes(0);
	    date.setSeconds(0);
	    date.setMilliseconds(0);
	    console.log('Moving files from current to past using date',date);
	    db.collection('current').find({date:{$lte: date}}).count(function(err,count){
		    if(count == 0){
			    console.log('No files found with date', date);
			    return;
		    }
			db.collection('current').find({date:{$lte: date}}).forEach(function(doc){
			    var time;
				//bulkInsert.insert(doc);
				//bulkRemove.find({_id:doc._id}).removeOne();
				
				bulkInsert.find({
		            "_id": doc._id,
		        })
		        .upsert()
		        .update(
		        {
		            $set: {tag:doc.tag, date:doc.date, var:doc.var, data:doc.data}
		        });
				
				counter ++;
				if( counter % x == 0 || counter == count){
					bulkInsert.execute()
					//bulkRemove.execute()
					bulkInsert = db.collection('past').initializeUnorderedBulkOp()
					//bulkRemove = db.collection('current').initializeUnorderedBulkOp()
				}
				if(counter == count){
					finish = new Date();
				    time = ((finish.getTime() - start.getTime()) / 1000);
				    console.log('Finished moving '+count+' documents to "past" collection in ',(finish.getTime() - start.getTime())/1000+'s');
				    console.log('Deletion process started.');
				    db.collection('current').deleteMany({date:{$lte: date}}, function(){
					    console.log('Finished deleting files.');
				    });
				}
			});
	    });
		
	});
}

http.createServer(function (request, response) {
    response.writeHead(200, {
        'Content-Type': 'text/plain',
        'Access-Control-Allow-Origin' : '*'
    });
    response.end('Hello World\n');
}).listen(port);

//server.listen(port, () => console.log('listening on port ' + port));

///////////////////////////////////////////////////////////////////////////////////////////////////////////
//******************************* IMPORTANT ******* IMPORTANT *******************************************//
///////////////////////////////////////////////////////////////////////////////////////////////////////////
/////// Garbage collector, server need to be started with the command ' node --expose-gc ./bin/www '///////
/////// This command has been added to the package.json scripts and can be started as ' npm start ' ///////
///////////// This manual garbage collection helps to reduce the footprint significantly //////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////////
//******************************* IMPORTANT ******* IMPORTANT *******************************************//
///////////////////////////////////////////////////////////////////////////////////////////////////////////
setInterval(function(){
  //Garbage collection every 5 seconds.
  global.gc();
}, 1000*5);
///////////////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////// End of Garbage Collection ///////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////////
