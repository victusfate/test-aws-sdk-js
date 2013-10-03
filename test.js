var fs      = require('fs');
var AWS     = require('aws-sdk');
var path    = require('path');

var sUploadPath = '/Users/messel/Desktop/Dropbox/lib/cameo_util/chainsaw_data/tmp/';
var sDownloadPath = '/Users/messel/Desktop/junk/';


// Instead, do this:
AWS.config.loadFromPath('./credentials.json');

// Set your region for future requests.
AWS.config.update({region: 'us-east-1'});

// Create a bucket using bound parameters and put something in it.
// var s3bucket = new AWS.S3({params: {Bucket: 'myBucket'}});

// Make sure to change the bucket name from "myBucket" to something unique.
var s3 = new AWS.S3({params: {Bucket: 'messel.test.cameo.tv'}});
// var data = {Key: 'myKey', Body: 'Hello!'};
// s3.putObject(data, function(err, data) {
//     if (err) {
//         console.log("Error uploading data: ", err);
//     } else {
//         console.log("Successfully uploaded data to messel.test.cameo.tv/myKey");
//         s3.getObject({Key: 'myKey'}, function(err, data) {
//             if (err) {
//                 console.log("Error uploading data: ", err);
//             } else {
//                 console.log("Successfully downloaded data from messel.test.cameo.tv/myKey ");
//                 console.log(data.Body.toString());
//             }
//         });        
//     }
// });


// var aTimeoutLevels = [10, 20, 30];
var fTimeOuts = function(sName,aLevels,fCallback) {
    var iTimeoutIndex  = 0;

    var iTimeout = setInterval(function() {
        iTimeoutIndex++;

        switch (true) {
            // First Warning.
            case iTimeoutIndex == aTimeoutLevels[0]:
                console.log({action: 'TimeWarning', 
                    warning: 'We have been waiting for ' + sName + ' for ' + iTimeoutIndex + ' seconds'});
                break;

            
            case iTimeoutIndex >= aTimeoutLevels[aTimeoutLevels.length - 1]:
                console.log({action: 'TimeFailure', 
                    error: new Error('We have been waiting for ' + sName +' for over ' + iTimeoutIndex + ' seconds')});
                clearInterval(iTimeout);
                fCallback(sName + ' Timeout');
                break;

            // Interim Errors
            case aTimeoutLevels.indexOf(iTimeoutIndex):
                console.log({action: 'TimeSternWarning', 
                    error: new Error('We have been waiting for ' + sName + ' for over ' + iTimeoutIndex + ' seconds')});
                break;
        }
    }, 1000);
}

fs.readdir(sUploadPath,function(err,aFiles) {
    aFiles.length = 2;
    var N = aFiles.length;

    var oFiles = {};
    for (var iFile in aFiles) {
        var sFile = aFiles[iFile];
        oFiles[sFile] = sFile;
    }


    if (err) {
        console.log('error reading directory ', sUploadPath);
    }
    else {
        var t1 = Date.now();
        var count = 0;
        var goingUp = true;

        var iTimeout = setInterval(function() {        
            if (count < N) {
                console.log('Still going count',count,'/',N,Date.now()-t1,Object.keys(oFiles).join(','));
            }
            else {
                if (goingUp) {
                    goingUp = false;

                    console.log('Done uploading at ',Date.now()-t1);
                    count = 0;
                    t1 = Date.now();
                    for (var iFile in aFiles) {
                        var sFile = aFiles[iFile];
                        oFiles[sFile] = sFile;
                    }

                    for (var iFile in aFiles) {
                        var down = function(sFile) {
                            console.log('sFile',sFile);

                            var file = fs.createWriteStream(path.join(sDownloadPath,sFile));
                            var s3ReadStream = s3.getObject({Key: sFile}).createReadStream();
                            s3ReadStream.pipe(file);
                            s3ReadStream.on('end',function(err) {
                                if (err) {
                                    clearInterval(iTimeout);
                                    console.log({ status:'s3ReadStream error',err:err });
                                }
                                else {
                                    file.on('close',function(err) {
                                        if (err) {
                                            clearInterval(iTimeout);
                                            console.log({ status:'file write stream error',err:err });
                                        }
                                        else {
                                            count++;
                                            console.log('Successfully downloaded file',sFile);
                                            delete oFiles[sFile];
                                        }
                                    });
                                }
                            });
                        };
                        down(aFiles[iFile]);
                    }
                }
                else {
                    console.log('Done uploading at ',Date.now()-t1);
                    clearInterval(iTimeout);
                }
            }
        }, 1000);


        for (var iFile in aFiles) {
            
            var up = function(sFile) {
                console.log('sFile',sFile);
                fs.readFile(path.join(sUploadPath,sFile), function(err,data) {
                    if (err) {
                        console.log('error reading file',sFile);
                        clearInterval(iTimeout);
                        return err;
                    }
                    else {
                        console.log('successfully read file',sFile);
                        var oUp = { Key: sFile, Body: data };
                        s3.putObject(oUp, function(err, data) {
                            count++;
                            if (err) {
                                console.log("Error uploading data: ", err);
                                clearInterval(iTimeout);
                            } 
                            else {
                                console.log(sFile,'made it up aok');
                            }
                        });
                    }
                });
            };
            up(aFiles[iFile]);
        }
    }
});