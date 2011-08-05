exports.run = function(profiler, config, callback) {
    var iterator = profiler.db.newIterator({}),
        count = 0;

    // iterate over the database
    for (iterator.seekToFirst(); iterator.valid(); iterator.next()) {
        var key = iterator.key().toString(),
            value = iterator.value().toString();
        
        count++;
    } // for
    
    callback({
        iterate: profiler.elapsed(),
        itemCount: count
    });
};