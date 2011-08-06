var path = require('path'),
    fs = require('fs'),
    DB = require('node-leveldb').DB,
    uuid = require('node-uuid'),
    reInvalidFile = /^\./;
    
function generateDocs(db, config, callback) {
    console.log('creating ' + config.count + ' random key entries');
    for (ii = 0; ii < config.count; ii++) {
        var id = uuid();

        db.put({}, new Buffer('test::' + id), new Buffer(JSON.stringify({
            id: id,
            name: 'Bob',
            age: 33
        })));
    } // for
    
    callback();
} // generateDocs

function loadFiles(db, config, callback) {
    var files = fs.readdirSync(config.files);
    
    
    files.forEach(function(bucket) {
        if (reInvalidFile.test(bucket)) {
            return;
        } // if
        
        var dataFiles = fs.readdirSync(path.join(config.files, bucket));

        console.log('loading ' + dataFiles.length + ' files for bucket: ' + bucket);
        dataFiles.forEach(function(filename) {
            if (reInvalidFile.test(filename)) {
                return;
            } // if

            var content = fs.readFileSync(path.join(config.files, bucket, filename), 'utf8');
            
            db.put(
                {}, 
                new Buffer(bucket + '::' + path.basename(filename, '.json')), 
                new Buffer(content)
            );
        });
    });

    callback();
} // loadFiles
    
exports.init = function(config, callback) {
    var start = Date.now(),
        dbPath = '/tmp/testdb-' + (config.files ? path.basename(config.files) : config.count || 500),
        lastKey,
        ii,
        db;
        
    // destroy the db if it already exists
    DB.destroyDB(dbPath, {});
    db = exports.open(config);
    
    (config.files ? loadFiles : generateDocs)(db, config, function() {
        // close the db
        db.close();

        callback({
            populate: Date.now() - start
        });
        
    });
};

exports.open = function(config) {
    var db = new DB(),
        dbPath = '/tmp/testdb-' + (config.files ? path.basename(config.files) : config.count || 500);
    
    // open the database
    db.open({
        create_if_missing: true
    }, dbPath);
    
    return db;
}; // open