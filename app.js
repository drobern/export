var express = require('express');
var app = express();
var zendesk = require('node-zendesk');

var moment = require('moment');
var util = require('util');
var _mysql = require('mysql');
var lineReader = require('readline');
var fs = require('fs');

var config = require('./config');

var HOST = config.host;
var PORT = config.port;
var MYSQL_USER = config.username;
var MYSQL_PASS = config.password;
var DATABASE = config.database;

var mysql = _mysql.createConnection({
    host: HOST,
    port: PORT,
    user: MYSQL_USER,
    password: MYSQL_PASS,
    multipleStatements: true,
});

var client = zendesk.createClient({
    username: config.zenuser,
    token: config.token,
    remoteUri: config.remoteUri
});

app.use(express.bodyParser({ keepExtensions:true, uploadDir: __dirname + '/public/downloads' }));
app.use(app.router);
app.use(express.static(__dirname + '/public'));

var interval = 180000;

function mysql_real_escape_string (str) {
    //console.log("THE STR: "+str);
    return str.replace(/[\0\x08\x09\x1a\n\r"'\\\%]/g, function (char) {
        switch (char) {
            case "\0":
                return "\\0";
            case "\x08":
                return "\\b";
            case "\x09":
                return "\\t";
            case "\x1a":
                return "\\z";
            case "\n":
                return "\\n";
            case "\r":
                return "\\r";
            case "\"":
            case "'":
            case "\\":
            case "%":
                return "\\"+char; // prepends a backslash to backslash, percent,
                                  // and double/single quotes
        }
    });
}

var dbInsert = function (body, solved_date, queries, callback) {
  console.log("NEW TICKET AT COUNT: "+t+" "+body.id+" "+body.subject+" "+body.ticket_type+" "+body.priority+" "+body.field_22789732+" "+body.field_22805997+" "+body.field_22799616+" "+body.created_at+" "+solved_date+" "+body.status);
  queries += util.format('INSERT INTO reporting VALUES(%d,"%s","%s","%s","%s","%s","%s","%s","%s","%s");',body.id,mysql_real_escape_string(body.subject),body.ticket_type,body.priority,body.field_22789732,body.field_22805997,body.field_22799616,body.created_at,solved_date,body.status);
  index[body.id] = t; 
  json = JSON.stringify({id: body.id, counter: t});
  writeIndex(json);
  t++
  callback(queries);
}; 

var dbUpdate = function (body, solved_date, queries, callback) {
  if (body.subject && body.status != 'Deleted') {
    console.log ('THE TICKET CHANGED FOR ID: '+body.id+' CREATED AT: '+body.created_at+' SOLVED AT: '+solved_date+' CUSTOMER: '+body.field_22805997+' STATUS: '+body.status);
    queries = 'UPDATE reporting SET subject = "'+mysql_real_escape_string(body.subject)+'", type = "'+body.ticket_type+'", priority = "'+body.priority+'", product = "'+body.field_22789732+'", customer = "'+body.field_22805997+'", category = "'+body.field_22799616+'", requested = "'+body.created_at+'", solved = "'+solved_date+'", status = "'+body.status+'"  where id='+body.id+';'
    callback(queries);
  } else {
    console.log ('THE TICKET DELETED FOR ID: '+body.id+' CREATED AT: '+body.created_at+' SOLVED AT: '+solved_date+' STATUS: '+body.status);
    queries += util.format('DELETE from reporting where id = "%d";', [body.id])
    delete index[body.id]; 
    callback(queries);
  }
};

var ticketMetric = function (id, ticket, queries, insert, callback) {
  client.ticketmetrics.list(id, function(err, statusList, body, responseList, resultList) {
    if (err) {
      console.log("ERROR METRIC API: "+err);
      return;
    }
    console.log('THE BODY: '+body.solved_at+' ID: '+ticket.id);
    if (insert) { 
      dbInsert(ticket, body.solved_at, queries, function(queries){
        callback(queries); 
      });
    } else {
      dbUpdate(ticket, body.solved_at, queries, function(queries){
        callback(queries); 
      });
    }
  });
};

var mysqlExec = function(queries) {
  mysql.query('use ' + DATABASE);
  var data1 = mysql.query(queries,  function selectCb(err, results, fields) {
    if (err) {
       throw err;
       response.end();
    }
    console.log("THE QUERY: "+queries);
    console.log("DATABASE UPDATED...");
  });
};

var writeTime = function(upData) {
  fs.writeFile (__file, upData, function(err) {
    if (err) 
      throw err;
    console.log('CYLCLE COMPLETED..');
  });
};

var writeIndex = function(json) {
  fs.appendFile (__index, json+'\n', function(err) {
    if (err) 
      throw err;
  });
}

function query(){
  var data = mysql.query('select 1 from reporting', function selectCb(err, results, fields) {
    if (err) {
       throw err;
       response.end();
    }
  });
};

var index = {};
var solved_date = null;
var t = 0;
var insert = true;
var start_time = 0;
var queries = '';
var __index = __dirname + "/index.db";
var __file = __dirname + "/start.db";
var rl = lineReader.createInterface({
  input: fs.createReadStream(__index)
});

rl.on('line', function (line) {
  d = JSON.parse(line);
  index[d['id']] = d['counter'];
  t++;
});

var ticket =  function() {
  fs.readFile(__file, function (err, data) {
    if (err) 
      throw err;
    console.log('DATA: '+data);
    var fields = data.toString().split('\n');
    var start_time = fields[0].toString().replace(/[\n\r]+/g, '');
    fiveMinute = Math.round((new Date() - (5 * 60 * 1000)) / 1000);
    console.log("START TIME: "+start_time+" FIVE MINUTE TIME: "+fiveMinute+" AND T: "+t);
    
    if (start_time > fiveMinute)
      start_time = fiveMinute;

    client.tickets.export(start_time, function(err, statusList, body, responseList, resultList) {
      if (err || !body.results) {
        console.log("ERROR IN API: "+err);
        return;
      }
      for (var i=0; i<body.results.length; i++) {
        if (body.results[i].id in index) { 
          insert = false;
          ticketMetric(body.results[i].id, body.results[i], queries, insert, function(sql) {
            mysqlExec(sql);
            queries = '';
          }); 
        } else {
          if (body.results[i].subject && body.results[i].status != 'Deleted') {
            insert = true;
            ticketMetric(body.results[i].id, body.results[i], queries, insert, function(sql) {
              mysqlExec(sql);
              queries = '';
            });
          }
        }
        solved_date = null;
      }
      start_time = body.end_time;
      upData = start_time+'\n'+t;
      console.log("THE NEW START TIME: "+start_time);
      writeTime(upData);
    }); 
  });
 };  

 rl.on('close', function() {
  ticket();
  setInterval(ticket, interval);
});

app.listen(3000);
