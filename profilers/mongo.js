var mongodb = require('mongodb');

exports.run = function(profiler, config, callback) {
    // open an connection to the local couchdb
    var server = new mongodb.Server(
            config.host || 'localhost',
            config.port || 27017,
            {}
        ),
        iterator = profiler.db.newIterator({}),
        data = {},
        collections = {};
        
    function cleanup(cleanupCallback) {
        cleanupCallback();
    } // cleanup        
        
    function readNext(client) {
        if (! iterator.valid()) {
            data.gets = profiler.elapsed();
            callback(data);
            return;
        } // if

        var key = profiler.parseKey(iterator.key().toString()),
            collection = new mongodb.Collection(client, key.bucket);

        collection.find({ id: key.id }).toArray(function(err, docs) {
            if (err) {
                data.readErrors = (data.readErrors || 0) + 1;
            } // if
            
            iterator.next();
            readNext(client);
        });
    } // readNext
        
    function writeNext(client) {
        if (! iterator.valid()) {
            data.puts = profiler.elapsed();

            // now read
            console.log('mongo: testing reads');
            iterator.seekToFirst();
            readNext(client);
            
            return;
        } // if

        var key = profiler.parseKey(iterator.key().toString()),
            collection = new mongodb.Collection(client, key.bucket),
            objectData = JSON.parse(iterator.value().toString());

        collection.insert({
            id: key.id,
            data: objectData
        }, function(err, docs) {
            if (err) {
                data.writeErrors = (data.writeErrors || 0) + 1;
            } // if
            
            iterator.next();
            writeNext(client);
        });
    } // writeNext
    
    new mongodb.Db('test', server, {}).open(function (error, client) {
        if (error) throw error;
        
        // drop the database
        client.dropDatabase(function(err) {
            if (! err) {
                console.log('mongo: testing writes');
                iterator.seekToFirst();
                writeNext(client);
            } // if
        });
    });
};