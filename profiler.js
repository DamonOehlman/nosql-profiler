var config = {
        count: 5000,
        
        profilers: {
            noop: {},
            
            couch: {
                enabled: false,
                host: 'localhost'
            },
            
            riak: {
                host: 'localhost'
            }
        }
    },
    fs = require('fs'),
    _ = require('underscore'),
    datafile = 'data/' + config.count + '.json',
    profilerDB = require('./profiler-db'),
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
        return;
    } // if

    if (! profiler) {
        profiler = createProfiler();
    } // if
    
    // open the profiler db
    profiler.db = profilerDB.open(config);
    
    if (! profileData[profilerName]) {
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
    } // if..else
} // runTests

fs.readFile(datafile, 'utf8', function(err, data) {
    if (err) {
        profilerDB.init(config, function() {
            // write the profile data
            fs.writeFile(datafile, JSON.stringify(profileData));
            
            // run the tests
            runProfiler();
        });
    }
    else {
        // parse the profile data
        profileData = JSON.parse(data);
        
        // run the tests
        runProfiler();
    } // if..else
});