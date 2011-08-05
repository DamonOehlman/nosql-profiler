var fs = require('fs'),
    path = require('path'),
    pathTmp = 'data/tmp';

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
            
        console.log('filesystem: cleaning up');
        fs.readdir(pathTmp, function(err, files) {
            for (var ii = 0; ii < files.length; ii++) {
                var dataFiles = fs.readdirSync(path.join(pathTmp, files[ii]));
                
                for (var fileIdx = 0; fileIdx < dataFiles.length; fileIdx++) {
                    fs.unlinkSync(path.join(pathTmp, files[ii], dataFiles[fileIdx]));
                } // for
                
                fs.rmdirSync(path.join(pathTmp, files[ii]));
            } // for
            
            data.cleaned = profiler.elapsed();
            callback(data);
        });
    } // cleanup        
        
    function readNext() {
        if (! iterator.valid()) {
            data.gets = profiler.elapsed();

            cleanup();
            return;
        } // if

        var key = profiler.parseKey(iterator.key().toString());
        
        fs.readFile(path.join(pathTmp, key.bucket, key.id + '.json'), 'utf8', function(err, fileData) {
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
            console.log('filesystem: testing reads');
            iterator.seekToFirst();
            readNext();
            
            return;
        } // if

        var key = profiler.parseKey(iterator.key().toString()),
            folderPath = path.join(pathTmp, key.bucket);
        
        if (! path.existsSync(folderPath)) {
            fs.mkdirSync(folderPath, 0777);
        } // if

        fs.writeFile(
            path.join(folderPath, key.id + '.json'), 
            iterator.value().toString(),
            function(err) {
                if (err) {
                    data.writeErrors = (data.writeErrors || 0) + 1;
                } // if
                
                iterator.next();
                writeNext();
            }
        );
    } // writeNext
    
    if (! path.existsSync(pathTmp)) {
        fs.mkdirSync(pathTmp, 0777);
    } // if
    
    console.log('filesystem: testing writes');
    iterator.seekToFirst();
    writeNext();
};