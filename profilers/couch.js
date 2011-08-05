var cradle = require('cradle'),
    dbs = {};

function getDB(conn, dbname, callback) {
    if (dbs[dbname]) {
        callback(dbs[dbname]);
    }
    else {
        var db = dbs[dbname] = conn.database(dbname);
        
        // if the database exists, then drop it and rebuild
        db.exists(function(err, exists) {
            if (! err) {
                // if the database exists, then destroy it
                if (exists) {
                    db.destroy(function() {
                        db.create();
                        callback(db);
                    });
                }
                else {
                    db.create();
                    callback(db);
                } // if..else        
            }
            else {
                console.log('error opening couch db: ' + dbname);
                process.exit();
            } // if..else
        });
        
    }
} // makeDB

exports.run = function(profiler, config, callback) {
    // open an connection to the local couchdb
    var conn = new cradle.Connection(
            config.host || 'localhost',
            config.port || 5984,
            { cache: false }
        ),
        db = conn.database('test'),
        iterator = profiler.db.newIterator({});
        data = {};
        
    function readNext() {
        if (! iterator.valid()) {
            data.gets = profiler.elapsed();

            callback(data);
            return;
        } // if
        
        var key = profiler.parseKey(iterator.key().toString());
        
        getDB(conn, key.bucket, function(db) {
            // save the data
            db.get(key.id, function(err, doc) {
                if (err) {
                    data.readErrors = (data.readErrors || 0) + 1;
                } // if
                

                iterator.next();
                readNext();
            });
        });        
    } // read
        
    function writeNext() {
        if (! iterator.valid()) {
            data.puts = profiler.elapsed();
            
            iterator.seekToFirst();
            
            console.log('couchdb: testing reads');
            readNext();
            
            return;
        } // if
        
        var key = profiler.parseKey(iterator.key().toString());
        
        getDB(conn, key.bucket, function(db) {
            // save the data
            db.save(
                key.id,
                JSON.parse(iterator.value().toString()), 
                function(err, res) {
                    if (err) {
                        data.writeErrors = (data.writeErrors || 0) + 1;
                    } // if
                    
                    iterator.next();
                    writeNext();
                }
            );
        });
    } // writeNext
    
    iterator.seekToFirst();
    
    console.log('couchdb: testing writes');
    writeNext();
};