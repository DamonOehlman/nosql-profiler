var DB = require('node-leveldb').DB,
    uuid = require('node-uuid'),
    path = '/tmp/testdb.db';
    
exports.init = function(callback) {
    var start = Date.now(),
        lastKey,
        ii,
        data = {
            count: 1000000
        },
        db;
        
    // destroy the db if it already exists
    DB.destroyDB(path, {});
    db = exports.open();
    
    
    console.log('creating ' + data.count + ' random key entries');
    for (ii = 0; ii < data.count; ii++) {
        var id = uuid();

        db.put({}, new Buffer(id), new Buffer(JSON.stringify({
            id: id,
            name: 'Bob',
            age: 33
        })));
    } // for
    
    // update the populate data stat
    data.populate = Date.now() - start;
    
    // now iterate the data
    console.log('benchmarking data iteration');

    // reset the start counter
    start = Date.now();

    // iterate over the test database
    var iterator = db.newIterator({});

    for (iterator.seekToFirst(); iterator.valid(); iterator.next()) {
        var key = iterator.key().toString('utf8');

        if (lastKey && lastKey > key) {
            console.log('found sorting error');
        } // if

        lastKey = key;
    } // for
    
    // update the iterate count
    data.iterate = Date.now() - start;
  
    // close the db
    db.close();
    
    if (callback) {
        callback(data);
    } // if
};

exports.open = function() {
    var db = new DB();
    
    // open the database
    db.open({
        create_if_missing: true
    }, path);
    
    return db;
}; // open