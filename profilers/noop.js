exports.run = function(profiler, config, callback) {
    var iterator = profiler.db.newIterator({});

    // iterate over the database
    for (iterator.seekToFirst(); iterator.valid(); iterator.next()) {
        var key = iterator.key().toString(),
            value = iterator.value().toString();
    } // for
    
    callback({
        iterate: profiler.elapsed()
    });
};