/**************************************************************************************

Copyright © 2004-2011 VMware, Inc. All rights reserved.

**************************************************************************************/

// This file contains basic performance assessments.

// Resolve dependencies.
var lib_mvstore = require('./lib/mvstore-client');
var lib_fs = require('fs');
var lib_sys = require('sys');

// Connect to the mvstore server.
var lMvStore = lib_mvstore.createConnection("http://perf01:@localhost:4560/db/", {keepalive:true});
var InstrSeq = lib_mvstore.instrSeq;
var NUM_PINS = 500;

// Define a helper to produce random strings.
function randomString(pLen)
{
  var lResult = "";
  var lChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
  for (var i = 0; i < pLen; i++)
    lResult += lChars.charAt(Math.floor(Math.random() * lChars.length));
  return lResult;
}

// Define the tests.
var lTests =
{
  bootstrap:function(pOnSuccess)
  {
    // This is just to make sure that the first real perf test doesn't incur any artificial overhead
    // due to opening the store or whatever...
    var lSS = new InstrSeq();
    lSS.push(function() { lMvStore.startTx(); lSS.next(); });
    lSS.push(function() { lMvStore.createPINs([{"http://localhost/mv/property/perf01/startedAt":new Date()}], lSS.next); });
    lSS.push(function() { lMvStore.commitTx(lSS.next); });
    lSS.push(pOnSuccess);
    lSS.start();
  },
  create_pins_pathsql:function(pOnSuccess)
  {
    if (!lMvStore.keptAlive())
    {
      console.log("warning: this test is designed primarily for keep-alive connection.");
    }
    var lSS = new InstrSeq();
    var lNumPins = 0;
    var lCreatePins_keepalive =
      function(_pE, _pR)
      {
        if (undefined != _pE)
          { console.log("ERROR: " + _pE); lSS.next(); return; }
        if (lNumPins++ >= NUM_PINS)
          { lSS.next(); return; }
        lMvStore.q("INSERT (perf01p:name, perf01p:code, perf01p:type) VALUES ('" + randomString(10) + "', '" + randomString(15) + "', 'pathsql');", lCreatePins_keepalive);
      }
    var lCreatePins_nokeepalive =
      function(_pE, _pR)
      {
        if (undefined != _pE)
          { console.log("ERROR: " + _pE); lSS.next(); return; }
        if (lNumPins++ >= NUM_PINS)
          { lSS.next(); return; }
        lMvStore.q("INSERT (\"http://localhost/mv/property/perf01/name\", \"http://localhost/mv/property/perf01/code\", \"http://localhost/mv/property/perf01/type\") VALUES ('" + randomString(10) + "', '" + randomString(15) + "', 'pathsql');", lCreatePins_nokeepalive);
      }
    if (lMvStore.keptAlive())
    {
      lSS.push(function() { lMvStore.q("SET PREFIX perf01p: 'http://localhost/mv/property/perf01/';", lSS.next); });
      lSS.push(function() { lMvStore.q("START TRANSACTION;", lSS.next); });
      lSS.push(lCreatePins_keepalive);
      lSS.push(function() { lMvStore.q("COMMIT;", lSS.next); });
    }
    else
    {
      lSS.push(lCreatePins_nokeepalive);
    }
    lSS.push(pOnSuccess);
    lSS.start();
  },
  create_pins_protobuf:function(pOnSuccess)
  {
    var lSS = new InstrSeq();
    var lNumPins = 0;
    var lCreatePins =
      function(_pE, _pR)
      {
        if (undefined != _pE)
          { console.log("ERROR: " + _pE); lSS.next(); return; }
        if (lNumPins++ >= NUM_PINS)
          { lSS.next(); return; }
        lMvStore.createPINs([{"http://localhost/mv/property/perf01/name":randomString(10), "http://localhost/mv/property/perf01/code":randomString(15), "http://localhost/mv/property/perf01/type":"proto"}], lCreatePins);
      }
    lSS.push(function() { lMvStore.startTx(); lSS.next(); });
    lSS.push(lCreatePins);
    lSS.push(function() { lMvStore.commitTx(lSS.next); });
    lSS.push(pOnSuccess);
    lSS.start();
  },
};

// Run the tests.
var lTestNames = new Array()
for (iT in lTests)
  lTestNames.push(iT);
var iTest = 0;
var lOnExit =
  function()
  {
    console.log("Exiting.");
    lMvStore.terminate();
  };
var lRunNextTest =
  function(pOnlyThisTest)
  {
    if (iTest >= lTestNames.length) { lOnExit(); return; }
    var _lTN = lTestNames[iTest];
    if (_lTN.slice(0, 1) == "_") { console.log("\nSkipping " + _lTN); iTest++; lRunNextTest(pOnlyThisTest); return; }
    if (undefined != pOnlyThisTest && pOnlyThisTest != _lTN) { iTest++; lRunNextTest(pOnlyThisTest); return; }
    console.log("\nRunning " + _lTN + ":");
    console.time(_lTN);
    lTests[_lTN](function(){ console.timeEnd(_lTN); iTest++; lRunNextTest(pOnlyThisTest); });
  };
lRunNextTest(/*"create_pins_protobuf"*/);
