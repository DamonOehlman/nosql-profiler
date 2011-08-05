var cradle = require('cradle');

exports.run = function(profiler, config, callback) {
    // open an connection to the local couchdb
    var conn = new cradle.Connection(config.host),
        db = conn.database('test'),
        iterator = profiler.db.newIterator({});
        data = {};
        
    function writeNext() {
        if (! iterator.valid()) {
            data.puts = profiler.elapsed();
            callback(data);
            
            return;
        } // if
        
        // save the data
        db.save(
            iterator.key().toString(),
            JSON.parse(iterator.value().toString()), 
            function(err, res) {
                if (! err) {
                    iterator.next();
                    writeNext();
                }
                else {
                    data.error = err;
                    callback(data);
                }
            }
        );
    } // writeNext
    
    function init() {
        // if the database exists, drop it and create it
        db.create();
        data.init = profiler.elapsed();

        iterator.seekToFirst();
        writeNext();
    } // init
    
    db.exists(function(err, exists) {
        if (! err) {
            // if the database exists, then destroy it
            if (exists) {
                db.destroy(init);
            }
            else {
                init();
            } // if..else        
        }
        else {
            callback({
                error: err
            });
        } // if..else
    });
};