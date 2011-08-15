exports.run = function(profiler, config, callback) {
    // open an connection to the local couchdb
    var couch = require('PJsonCouch')({
            protocol: 'http',
            host: config.host || 'localhost',
            port: config.port || 5984,
            path: config.path || ''
        }),
        iterator = profiler.db.newIterator({}),
        data = {};
        
    function readNext() {
        if (! iterator.valid()) {
            data.gets = profiler.elapsed();

            callback(data);
            return;
        } // if
        
        couch.getDoc({ id: iterator.key().toString() }, function(res) {
            if (res.error) {
                data.writeErrors = (data.writeErrors || 0) + 1;
            } // if
            
            iterator.next();
            readNext();
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
        
        var doc = JSON.parse(iterator.value().toString());
        doc._id = iterator.key().toString();
        
        couch.saveDoc({ doc: doc }, function(res) {
            if (res.error) {
                data.writeErrors = (data.writeErrors || 0) + 1;
            } // if
            
            if (res.debug) {
                console.log('error writing: ' + doc._id);
                console.log(doc);
            } // if

            iterator.next();
            writeNext();
        });
    } // writeNext
    
    couch.setDebugConfig({
        debugOnError: true
    });
    
    // delete the db
    couch.setDB({ db: 'tripplanner' });
    couch.deleteDB({}, function(res) {
        couch.createDB({}, function(res) {
            if (! res.error) {
                iterator.seekToFirst();

                console.log('couchdb: testing writes');
                writeNext();
            } // if
        });
    });
};