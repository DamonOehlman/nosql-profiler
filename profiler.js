var fs = require('fs'),
    datafile = 'profile.json',
    profilerDB = require('./profiler-db'),
    profileData = {},
    profilers = ['couch', 'riak'],
    profilerIdx = 0;

function runProfiler() {
    if (profilerIdx >= profilers.length) {
        return;
    } // if
    
    var db = profilerDB.open(),
        profilerName = profilers[profilerIdx++];
        
    // require the specified profiler and run it
    console.log('running ' + profilerName + ' profiler');
    require('./profilers/' + profilerName).run(db, function(data) {
        profileData[profilerName] = data;
        
        // close the profilerdb
        db.close();
        
        // write the profile data
        fs.writeFile(datafile, JSON.stringify(profileData));
        
        // run the profiler again
        runProfiler();
    });
} // runTests
    
    
fs.readFile(datafile, 'utf8', function(err, data) {
    if (err) {
        profilerDB.init(function(data) {
            // update the profile data
            profileData.testData = data;
            
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