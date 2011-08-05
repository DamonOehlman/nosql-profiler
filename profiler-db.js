var DB = require('node-leveldb').DB,
    uuid = require('node-uuid');
    
exports.init = function(config, callback) {
    var start = Date.now(),
        path = '/tmp/testdb-' + config.count,
        lastKey,
        ii,
        db;
        
    // destroy the db if it already exists
    DB.destroyDB(path, {});
    db = exports.open(config);
    
    
    console.log('creating ' + config.count + ' random key entries');
    for (ii = 0; ii < config.count; ii++) {
        var id = uuid();

        db.put({}, new Buffer('test::' + id), new Buffer(JSON.stringify({
            id: id,
            name: 'Bob',
            age: 33
        })));
    } // for
    
    // close the db
    db.close();
    
    callback({
        populate: Date.now() - start
    });
};

exports.open = function(config) {
    var db = new DB(),
        path = '/tmp/testdb-' + config.count;
    
    // open the database
    db.open({
        create_if_missing: true
    }, path);
    
    return db;
}; // open