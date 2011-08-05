var client = require('redis').createClient();

exports.run = function(profiler, config, callback) {
    // open an connection to the local couchdb
    var db = require('riak-js').getClient({
            host: config.host || 'localhost',
            port: config.port || 8098
        }),
        iterator = profiler.db.newIterator({});
        data = {};
        
    function cleanup() {
        var counter = 0,
            removeTimeout = 0;

        console.log('redis: cleaning up');
        client.quit();
        callback(data);
    } // cleanup        
        
    function readNext() {
        if (! iterator.valid()) {
            data.gets = profiler.elapsed();

            cleanup();
            return;
        } // if

        var key = profiler.parseKey(iterator.key().toString());
        
        client.hgetall(key.bucket + '::' + key.id, function(err, itemData) {
            if (err) {
                data.readErrors = (data.readErrors || 0) + 1;
            } // if
            
            iterator.next();
            readNext();
        });
    } // readNext
        
    function writeNext() {
        if (! iterator.valid()) {
            data.puts = profiler.elapsed();

            // now read
            console.log('redis: testing reads');
            iterator.seekToFirst();
            readNext();
            
            return;
        } // if

        var key = profiler.parseKey(iterator.key().toString());
        
        client.hmset(
            key.bucket + '::' + key.id,
            JSON.parse(iterator.value().toString()), 
            function(err) {
                if (err) {
                    data.writeErrors = (data.writeErrors || 0) + 1;
                } // if
                
                iterator.next();
                writeNext();
            }
        );
    } // writeNext
    
    console.log('redis: testing writes');
    iterator.seekToFirst();
    writeNext();
};