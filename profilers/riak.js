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

        console.log('riak: cleaning up');
        db.buckets(function(err, bucketData) {
            if (err) {
                callback();
                return;
            } // if
            
            bucketData.buckets.forEach(function(bucket) {
                console.log('riak: emptying bucket - ' + bucket);
                
                db.getAll(bucket, function(err, items) {
                    for (var ii = 0; (! err) && ii < items.length; ii++) {
                        db.remove(bucket, items[ii].meta.key);
                    } // for
                    
                    clearTimeout(removeTimeout);
                    removeTimeout = setTimeout(function() {
                        data.cleaned = profiler.elapsed();
                        callback(data);
                    }, 100);
                });
            });
        });
    } // cleanup        
        
    function readNext() {
        if (! iterator.valid()) {
            data.gets = profiler.elapsed();

            cleanup();
            return;
        } // if

        var key = profiler.parseKey(iterator.key().toString());
        
        db.get(key.bucket, key.id, function(err, itemData, meta) {
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
            console.log('riak: testing reads');
            iterator.seekToFirst();
            readNext();
            
            return;
        } // if

        var key = profiler.parseKey(iterator.key().toString());
        
        db.save(
            key.bucket, 
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
    } // writeNext
    
    console.log('riak: testing writes');
    iterator.seekToFirst();
    writeNext();
};