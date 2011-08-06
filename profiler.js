var path = require('path'),
    fs = require('fs'),
    _ = require('underscore'),
    profilerDB = require('./profiler-db'),
    config = {
        files: '/development/data/nosql-test',
        count: undefined, // 500,
        
        profilers: {
            noop: {},
            filesystem: {
                enabled: false
            },
            
            couch: {
                enabled: true,
                host: 'localhost'
            },
            
            riak: {
                enabled: true,
                host: 'localhost'
            },
            
            redis: {
                enabled: true
            },
            
            mongo: {
                enabled: false,
                host: 'localhost'
            }
        }
    },
    datafile = 'data/' + (config.files ? path.basename(config.files) : config.count || 500) + '.json',
    profileData = {
        profiles: {}
    },
    profilers = _.keys(config.profilers),
    profilerIdx = 0,
    profiler;
    
function createProfiler() {
    var ticks = Date.now();
    
    return profiler = {
        elapsed: function() {
            var value = Date.now() - ticks;
            ticks = Date.now();
            
            return value;
        },
        
        parseKey: function(key) {
            var parts = key.split('::');
            
            return {
                bucket: parts.length > 1 ? parts[0] : 'test',
                id: parts[parts.length > 1 ? 1 : 0]
            };
        }
    };
} // createProfiler

function runProfiler() {
    if (profilerIdx >= profilers.length) {
        return;
    } // if
    
    var profilerName = profilers[profilerIdx++],
        profilerConf = config.profilers[profilerName];
        
    // if the profiler is disabled, then exit
    if (typeof profilerConf.enabled != 'undefined' && !profilerConf.enabled) {
        runProfiler();
        return;
    } // if

    if (! profiler) {
        profiler = createProfiler();
    } // if
    
    // open the profiler db
    profiler.db = profilerDB.open(config);
    
    if (profilerConf.always || (! profileData.profiles[profilerName])) {
        // require the specified profiler and run it
        console.log('running ' + profilerName + ' profiler');
        
        require('./profilers/' + profilerName).run(profiler, profilerConf, function(data) {
            profileData.profiles[profilerName] = data;

            // close the profilerdb
            profiler.db.close();

            // write the profile data
            profileData.config = config;
            fs.writeFile(datafile, JSON.stringify(profileData));

            // run the profiler again
            runProfiler();
        });
    }
    else {
        console.log('profile for ' + profilerName + ' already generated');
        runProfiler();
    } // if..else
} // runTests

profilerDB.init(config, function() {
    fs.readFile(datafile, 'utf8', function(err, data) {
        if (! err) {
            profileData = JSON.parse(data);
        } // if
        
        // run the tests
        runProfiler();
    });
});