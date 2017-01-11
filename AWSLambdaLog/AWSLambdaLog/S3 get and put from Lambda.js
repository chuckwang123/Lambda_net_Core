// dependencies
var async = require('async');
var aws = require('aws-sdk');
var moment = require('moment');
moment().format();

var s3Bucket = 'junyuevernote';
var s3 = new aws.S3({params: {Bucket: s3Bucket}});

//after event
exports.handler = function (event, context, callback) {
    //var srcBucket = event.Records[0].s3.bucket.name;
    var srcKey = event.Records[0].s3.object.key;
    var settings = getOutputSettings(srcKey);

    // Download the file from S3, transform, and upload to a different S3 bucket.
    async.waterfall([
        function (next) {
            try {
                    download(settings, next);
                } catch(err) {
                    next(JSON.stringify({name: err.name, message: err.message, fun: "download"}));
                }
        },
        function (response, next) {
            try {
                    tranform(response, settings, next);
                } catch(err) {
                    next(JSON.stringify({name: err.name, message: err.message, fun: "tranform"}));
                }
        },
        function (splitdata, next) {
                try {
                    uploadRequests(splitdata, settings, next);
                } catch(err) {
                    next(JSON.stringify({name: err.name, message: err.message, fun: "uploadRequests"}));
                }
        },
        function(splitdata, next){
            try{
                    passToKinesis(splitdata,settings,next);
            }catch(err){
                next(JSON.stringify({name: err.name, message: err.message, fun: "uploadRequests"}));
            }
        }
    ], function (err) {
        if (err) {
            console.error(
                'Unable to ' + s3Bucket + '/' + srcKey +
                   ' due to an error: ' + err
            );
        } else {
            console.log(
                'Successfully resized ' + s3Bucket + '/' + srcKey 
            );
        }
        
        callback(null, "message");
    }
    );
};

function download(settings, next) {
    console.log("download: " + settings.srcKey);

    s3.getObject({ Key: settings.srcKey }, function(err, data) {
        if (err) {
            next('download:s3.getObject(' + settings.srcKey + '): ' + err);
        } else {
            next(null, data);
        }
    });
}

function tranform(response, settings, next) {
    console.log("tranform: " + settings.srcKey);
    
    var responseBody = response.Body ? response.Body.toString() : '';

    if (responseBody.length < 1) {
        next('transform: file is empty');
        return;
    }
    
    var request;
    var splitdata = [];
    var SearchLog;
    var InfoLog;
    var DetailLog;
    try {
        var responseArr = [];
        responseArr = responseBody.split("\n");
        for (var i = 0; i < responseArr.length-1; i++){
            responseItem = responseArr[i];
            //split the tab
            var responseItemArr = responseItem.split(/\t/);
            //get the file name
            var foldername = responseItemArr[5].split("^",1)[0];
            //todo case sensitive verify folder name in search count info item detail status reloadstream reloadlicense 
            if(verifyRequestType(foldername) === false){
                 //send to error folder   
                AddtoErrorFolder(splitdata,responseItem);
                continue;
            }
            //todo verify
            if(verifyDateTime(responseItemArr) === false){
                //send to error folder
                AddtoErrorFolder(splitdata,responseItem);
                continue;
            }

            if (splitdata.hasOwnProperty(foldername) === false) {
                splitdata[foldername] = responseItem;
            }
            else{
                splitdata[foldername] = splitdata[foldername] + "\n" + responseItem;                
            }
        }
        
    } catch (e) {
        next('unable to split response body of file ' + s3Bucket + '/' + settings.srcKey + ': ' + e.message);
        return;
    }
    next(null,splitdata);
}

function verifyRequestType(foldername){
    //if it's not, put into error folder
    var folder = foldername.toLowerCase();
    var acceptName  =["search","count","info","item","detail","status","reloadstream","reloadlicense"];
    if (acceptName.indexOf(folder) >= 0) {
        return true;
    }
    else{
        return false;
    }
}

function verifyDateTime(responseItemArr){
    //verify column2 the datetime format(yy-mm-dd: time; mm-dd-yy:time) 
    var datetimeValue = responseItemArr[1] + " " + responseItemArr[2];
    if((moment(datetimeValue,"YYYY-MM-DD HH:mm:ss.SSS Z",true).isValid()) || (moment(datetimeValue,"MM-DD-YYYY HH:mm:ss.SSS Z",true).isValid()) ){
        return true;
    }
    else{
        return false;
    }
}

function AddtoErrorFolder(splitdata,responseItem){
    console.log(responseItem);
    if (splitdata.hasOwnProperty("error") === false) {
        splitdata["error"] = responseItem;
    }
    else{
        splitdata["error"] = splitdata["error"] + "\n" + responseItem;                
    }
}

function getOutputSettings(srcKey) {
    var settings = {};
    settings.srcKey = "InputFolder/" + srcKey;
    settings.dstKey = srcKey;
    return settings;
}

function uploadRequests(splitdata, settings, next) {
    console.log("uploadRequests: " + settings.dstKey);

    try{

        for (var key in splitdata) {
            var params = {
                Bucket: s3Bucket, 
                Key: "OutputFolder/" + key +"/"+ settings.dstKey, 
                Body: splitdata[key]
            };
            s3.putObject(params, function(err, data) {
                   if (err) {
                    next('uploadRequests:s3.putRecord(' + settings.dstKey + '): ' + err);
                } 
            });
        }
    }catch(e){
        next('unable to upload response body of file ' + params.Bucket + '/' + params.Key + ': ' + e.message);
        return;
    }
    next(null,splitdata);
}

function passToKinesis(splitdata, settings, next){
    console.log("upload Data to Kinesis: " + settings.srcKey);
    var params = {
        Records: splitdata 
    };
    //need to identify Firehose
    Firehose.putRecord(params, function(err, data) {
        if (err) {
            next('uploadProblems:problemsFirehose.putRecordBatch(' + settings.FirehoseKey + '): ' + err);
        } else {
            next(null);
        }
    });
}