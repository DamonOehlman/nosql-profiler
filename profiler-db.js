var path = require('path'),
    fs = require('fs'),
    uuid = require('node-uuid'),
    _ = require('underscore'),
    reInvalidFile = /^\./;
    
var db = (function() {
    var data = {};
    
    function Iterator() {
        var keys = _.keys(data),
            keyIdx = 0;
        
        return {
            key: function() {
                return new Buffer(keys[keyIdx]);
            },
            
            value: function() {
                return new Buffer(data[keys[keyIdx]]);
            },
            
            seekToFirst: function() {
                keyIdx = 0;
            },
            
            valid: function() {
                return keyIdx >= 0 && keyIdx < keys.length;
            },
            
            next: function() {
                keyIdx++;
            }
        };
    } // Iterator
    
    return {
        close: function() {
        },
        
        newIterator: function() {
            return new Iterator();
        },
        
        get: function(key) {
            return data[key];
        },
        
        put: function(key, obj) {
            data[key] = obj;
        }
    };
})();
    
function generateDocs(config, callback) {
    console.log('creating ' + config.count + ' random key entries');
    for (ii = 0; ii < config.count; ii++) {
        var id = uuid();

        db.put('test::' + id, JSON.stringify({
            id: id,
            name: 'Bob',
            age: 33
        }));
    } // for
    
    callback();
} // generateDocs

function loadFiles(config, callback) {
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
            
            /*
            // replace troublesome characters
            content = content
                .replace(/\u2019|\u2018/g, '\'')
                .replace(/\u00a9/g, '&copy;')
                .replace(/\u00b0/g, '&deg;')
                .replace(/\u00ae/g, '&reg;')
                .replace(/\ufffd/g, '')
                .replace(/(ch).teau/ig, '$1ateau')
            */
            
            // update the bucket
            db.put(bucket + '::' + path.basename(filename, '.json'), content);
        });
    });

    callback();
} // loadFiles
    
exports.init = function(config, callback) {
    var start = Date.now(),
        dbPath = '/tmp/testdb-' + (config.files ? path.basename(config.files) : config.count || 500),
        lastKey,
        ii;
        
    (config.files ? loadFiles : generateDocs)(config, function() {
        callback({
            populate: Date.now() - start
        });
    });
};

exports.open = function(config) {
    return db;
}; // open