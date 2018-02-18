'use strict';

// this code follows exactly Emarsys nodejs template except everything related to fs module
const fs = require('fs');
const crypto = require('crypto');
const iso8601 = require('iso8601');
const request = require('request');
const timestamp = require('console-timestamp');

const user = 'bamilo_predict';
const secret = 'OCoruZGR4S4uPEZF0L16';
const json_to_feed = JSON.parse(fs.readFileSync('C:/Projects/Marketing/emarsys_api/temp/json_to_feed.txt', 'utf8'));

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

// delete file named 'json_to_feed.txt'
fs.unlink('C:/Projects/Marketing/emarsys_api/temp/json_to_feed.txt', function (err) {
    if (err) {
      console.log('Error deleting json_to_feed.txt: ',err);
      console.log('DD-MM-YY hh:mm:ss'.timestamp);
    } else {
      console.log('json_to_feed deletion time:');
      console.log('DD-MM-YY hh:mm:ss'.timestamp);
      console.log('json_to_feed deletion status: successful!');
    }
  }); 


  }
}
);