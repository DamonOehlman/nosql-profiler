exports.run = function(profiler, config, callback) {
    // open an connection to the local couchdb
    var db = require('riak-js').getClient({
            host: config.host || 'localhost',
            port: config.port || 8098
        }),
        iterator = profiler.db.newIterator({});
        data = {};
        
    function cleanup(cleanupCallback) {
        var counter = 0,
            removeTimeout = 0,
            buckets = [],
            bucketIdx = 0;
            
        function emptyNextBucket() {
            var bucket = buckets[bucketIdx++];
            
            console.log('riak: emptying bucket - ' + bucket);
            db.getAll(bucket, function(err, items) {
                if (! err) {
                    console.log('riak: removing ' + items.length + ' items');

                    for (var ii = 0; (! err) && ii < items.length; ii++) {
                        db.remove(bucket, items[ii].meta.key);
                    } // for
                } // if
                
                if (bucketIdx < buckets.length) {
                    emptyNextBucket();
                }
                else {
                    data.cleaned = profiler.elapsed();
                    cleanupCallback();
                } // if..else
            });
        } // emptyNextBucket

        console.log('riak: cleaning up');
        db.buckets(function(err, bucketData) {
            if (err || bucketData.buckets.length === 0) {
                cleanupCallback();
                return;
            } // if
            
            buckets = bucketData.buckets;
            emptyNextBucket();
        });
    } // cleanup        
        
    function readNext() {
        if (! iterator.valid()) {
            data.gets = profiler.elapsed();
            callback(data);
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
    
    cleanup(function() {
        console.log('riak: testing writes');
        iterator.seekToFirst();
        writeNext();
    });
};