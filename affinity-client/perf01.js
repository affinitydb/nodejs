/*
Copyright (c) 2004-2013 GoPivotal, Inc. All Rights Reserved.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,  WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
License for the specific language governing permissions and limitations
under the License.
*/

// This file contains basic performance assessments.

// Resolve dependencies.
var lib_affinity = require('./lib/affinity-client');
var lib_fs = require('fs');
var lib_sys = require('sys');

// Connect to the Affinity server.
var lAffinity = lib_affinity.createConnection("http://perf01:@localhost:4560/db/", {keepalive:true});
var InstrSeq = lib_affinity.instrSeq;
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
    lSS.push(function() { lAffinity.startTx(); lSS.next(); });
    lSS.push(function() { lAffinity.createPINs([{"http://localhost/afy/property/perf01/startedAt":new Date()}], lSS.next); });
    lSS.push(function() { lAffinity.commitTx(lSS.next); });
    lSS.push(pOnSuccess);
    lSS.start();
  },
  create_pins_pathsql:function(pOnSuccess)
  {
    if (!lAffinity.keptAlive())
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
        lAffinity.q("INSERT (perf01p:name, perf01p:code, perf01p:type) VALUES ('" + randomString(10) + "', '" + randomString(15) + "', 'pathsql');", lCreatePins_keepalive);
      }
    var lCreatePins_nokeepalive =
      function(_pE, _pR)
      {
        if (undefined != _pE)
          { console.log("ERROR: " + _pE); lSS.next(); return; }
        if (lNumPins++ >= NUM_PINS)
          { lSS.next(); return; }
        lAffinity.q("INSERT (\"http://localhost/afy/property/perf01/name\", \"http://localhost/afy/property/perf01/code\", \"http://localhost/afy/property/perf01/type\") VALUES ('" + randomString(10) + "', '" + randomString(15) + "', 'pathsql');", lCreatePins_nokeepalive);
      }
    if (lAffinity.keptAlive())
    {
      lSS.push(function() { lAffinity.q("SET PREFIX perf01p: 'http://localhost/afy/property/perf01/';", lSS.next); });
      lSS.push(function() { lAffinity.q("START TRANSACTION;", lSS.next); });
      lSS.push(lCreatePins_keepalive);
      lSS.push(function() { lAffinity.q("COMMIT;", lSS.next); });
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
        lAffinity.createPINs([{"http://localhost/afy/property/perf01/name":randomString(10), "http://localhost/afy/property/perf01/code":randomString(15), "http://localhost/afy/property/perf01/type":"proto"}], lCreatePins);
      }
    lSS.push(function() { lAffinity.startTx(); lSS.next(); });
    lSS.push(lCreatePins);
    lSS.push(function() { lAffinity.commitTx(lSS.next); });
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
    lAffinity.terminate();
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
