'use strict';

// this code follows exactly Emarsys nodejs template except everything related to fs module
const fs = require('fs');
const crypto = require('crypto');
const iso8601 = require('iso8601');
const request = require('request');
const timestamp = require('console-timestamp');
const promiseRetry = require('promise-retry');

// weird stuff: for require "./" redirects to folder of etl.js BUT "fs.readFileSync" redirects to folder of etl_nodejs_launcher.bat!! >< Hence I both put them in same folder
// EVEN WEIRDER, if I execute etl.js FROM R, it takes the same working directory as R for "fs.readFileSync"!!! >< 
// hence ./nodejs/temp NOT ./temp because wd of R is emarsys_api NOT nodejs folder
const config = require('./config.json');
const user = config.user;
const secret = config.secret;
const json_to_feed = JSON.parse(fs.readFileSync('./nodejs/temp/json_to_feed.txt', 'utf8'));

function getWsseHeader(user, secret) {
  let nonce = crypto.randomBytes(16).toString('hex');
  let timestamp = iso8601.fromDate(new Date());

  let digest = base64Sha1(nonce + timestamp + secret);

  return `UsernameToken Username="${user}", PasswordDigest="${digest}", Nonce="${nonce}", Created="${timestamp}"`
};

function base64Sha1(str) {
  let hexDigest = crypto.createHash('sha1')
    .update(str)
    .digest('hex');

  return new Buffer(hexDigest).toString('base64');
};

console.log('API upload start time:');
console.log('DD-MM-YY hh:mm:ss'.timestamp);
request.put({
  url: 'https://suite16.emarsys.net/api/v2/contact/?create_if_not_exists=0',
  headers: {
    'Content-Type': 'application/json',
    'X-WSSE': getWsseHeader(user, secret)
  },
  body: JSON.stringify(json_to_feed)
}, function(err, response, body) {
  if (err) {
    console.log('DD-MM-YY hh:mm:ss'.timestamp);
    console.error(err);
  } else {
    console.log('API upload result time:');
    console.log('DD-MM-YY hh:mm:ss'.timestamp);
    console.log('Response Status: ' + response.statusCode);
    console.log('Response Body: ', body);
    console.log('API upload result time (1 second before json_to_feed.txt deletion time):');
    console.log('DD-MM-YY hh:mm:ss'.timestamp);

    // NB: even if R is checking that 'json_to_feed.txt' still exists, nodejs can access 'json_to_feed.txt'!
   
      // wait 1 second
      setTimeout(function() {

      // delete file named 'json_to_feed.txt' and handle errors
      fs.unlink('./nodejs/temp/json_to_feed.txt', function (err) {
        if (err) {
          console.log('Error deleting json_to_feed.txt: ',err);
          console.log('DD-MM-YY hh:mm:ss'.timestamp);
        } else {
          console.log('json_to_feed deletion time:');
          console.log('DD-MM-YY hh:mm:ss'.timestamp);
          console.log('json_to_feed deletion status: successful!');
        }
      }); 
    }, 1000);
  




  }
}
);