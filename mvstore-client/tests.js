/**************************************************************************************

Copyright © 2004-2011 VMware, Inc. All rights reserved.

**************************************************************************************/

// This test file contains a blend of basic tests/samples, meant
// to verify and demonstrate basic mvStore functionality (in node.js).
// Here, the reader can expect to find verifiable answers to many of his basic
// "how to" questions. The file also contains a few longer examples, to give
// a sense of mvStore's typical workflows, in the context of "real" applications.
// It does not replace other existing test suites, such as the kernel test suite
// or the SQL compliance suite (both of which are much more exhaustive).
// But it should constitute a reasonably comprehensive (yet compact)
// first experience of interacting with mvStore in node.js.

// Resolve dependencies.
var lib_mvstore = require('./lib/mvstore-client');
var lib_assert = require('assert');
var lib_fs = require('fs');
var lib_sys = require('sys');

// Connect to the mvstore server.
var lMvStore = lib_mvstore.createConnection("http://nodetests:@localhost:4560/db/", {keepalive:false});
var InstrSeq = lib_mvstore.instrSeq;

// Define a small helper for simple write-only transactions.
function doSimpleTx(_pDoSync, _pCallback, _pTxLabel/*optional*/)
{
  lMvStore.startTx(_pTxLabel);
  var lDoCommit = _pDoSync();
  lDoCommit ? lMvStore.commitTx(_pCallback) : lMvStore.rollbackTx(_pCallback);
}

// Define a small helper to assert expected typical valid responses.
function assertValidResult(_pResult)
{
  lib_assert.ok(undefined != _pResult && _pResult instanceof Array);
}

// Define the tests.
var lTests =
{
  /**
   * Trivial tests (quick checks / examples).
   */
  
  test_basic_mvsql:function(pOnSuccess) // Pure mvsql with json responses.
  {
    var lSS = new InstrSeq();
    var lPID;
    lSS.push(function() { lMvStore.mvsql("INSERT (test_basic_mvsql__name, test_basic_mvsql__profession) VALUES ('Roger', 'Painter');", function(_pE, _pR) { assertValidResult(_pR); lPID = _pR[0].id; lSS.simpleOnResponse(_pE, _pR); }); });
    lSS.push(function() { lMvStore.mvsql("SELECT * WHERE EXISTS(test_basic_mvsql__name);", function(_pE, _pR) { assertValidResult(_pR); var _lFound = false; for (var _iP = 0; _iP < _pR.length; _iP++) { if (lPID == _pR[_iP].id) _lFound = true; } lib_assert.ok(_lFound); lSS.simpleOnResponse(_pE, _pR); }); });
    lSS.push(function() { console.log("done."); pOnSuccess(); });
    lSS.start();
  },
  test_basic_protobuf:function(pOnSuccess) // Protobuf-in, mvsql with protobuf-out, basic PIN interface.
  {
    var lSS = new InstrSeq();
    var lPID, lPINs;
    var lOnObjects = function(_pE, _pR) { var _lR = ""; lPID = _pR[0].pid; _pR.forEach(function(__pEl){ _lR += JSON.stringify(__pEl.toPropValDict()); }); lSS.simpleOnResponse(_pE, _lR);}
    lSS.push(function() { lMvStore.createPINs([{test_basic_protobuf__a_string:"whatever", test_basic_protobuf__a_number:123, test_basic_protobuf__a_date:new Date(), test_basic_protobuf__an_array:[1, 2, 3, 4]}], lOnObjects); });
    lSS.push(function() { lMvStore.mvsqlProto("SELECT * FROM @" + lPID.toString(16) + ";", function(_pE, _pR) { lPINs = _pR; lSS.simpleOnResponse(null, "found " + _pR.length + " results."); }); });
    lSS.push(
      function()
      {
        if (0 == lPINs.length) { lSS.next(); return; }
        doSimpleTx(
          function()
          {
            lPINs[0].set("test_basic_protobuf__x", 202020);
            lPINs[0].set("test_basic_protobuf__y", 303030);
            lPINs[0].get("test_basic_protobuf__an_array").push(5);
            return true; // Commit.
          },
          lSS.next);
      });
    lSS.push(function() { lPINs[0].refresh(lSS.simpleOnResponse); }); // Could automate something like this, on transactions that sacrificed some immediate updates (e.g. new eids).
    lSS.push(function() { lMvStore.mvsql("SELECT * WHERE EXISTS(test_basic_protobuf__an_array);", lSS.simpleOnResponse); });       
    lSS.push(function() { console.log("done."); pOnSuccess(); });
    lSS.start();
  },

  /**
   * Simple tests for collections (via protobuf).
   * Any property can become a collection.
   * In node.js, collections are exposed with an interface similar to the javascript Array's.
   */

  test_push_pop:function(pOnSuccess)
  {
    var lSS = new InstrSeq();
    var lPID, lPINs;
    var lOnObjects = function(_pE, _pR) { var _lR = ""; lPID = _pR[0].pid; _pR.forEach(function(__pEl){ _lR += JSON.stringify(__pEl.toPropValDict()); }); lSS.simpleOnResponse(_pE, _lR);}
    lSS.push(function() { lMvStore.createPINs([{pushpoptest:[1,2,3,4,5,6,7,8]}], lOnObjects); });
    lSS.push(function() { lMvStore.mvsqlProto("SELECT * FROM @" + lPID.toString(16) + ";", function(_pE, _pR) { lPINs = _pR; lSS.simpleOnResponse(null, "found " + _pR.length + " results."); }); });
    lSS.push(
      function()
      {
        if (0 == lPINs.length) { lSS.next(); return; }
        lib_assert.deepEqual(lPINs[0].toPropValDict()["pushpoptest"], [1,2,3,4,5,6,7,8], "poptest before");
        console.log("Performing 2 pops.");
        doSimpleTx(
          function()
          {
            var _lV1 = lPINs[0].get("pushpoptest").pop();
            lib_assert.deepEqual(_lV1, 8);
            var _lV2 = lPINs[0].get("pushpoptest").pop();
            lib_assert.deepEqual(_lV2, 7);
            return true; // Commit.
          },
          function()
          {
            lib_assert.deepEqual(lPINs[0].toPropValDict()["pushpoptest"], [1,2,3,4,5,6], "poptest after (mem)");
            lSS.next();
          });
      });
    lSS.push(function() { lPINs[0].refresh(lSS.simpleOnResponse); });
    lSS.push(function() { lib_assert.deepEqual(lPINs[0].toPropValDict()["pushpoptest"], [1,2,3,4,5,6], "poptest after (disk)"); lSS.next(); });
    lSS.push(
      function()
      {
        if (0 == lPINs.length) { lSS.next(); return; }
        lib_assert.deepEqual(lPINs[0].toPropValDict()["pushpoptest"], [1,2,3,4,5,6], "pushtest before");
        console.log("Performing 2 pushes.");
        doSimpleTx(
          function()
          {
            lPINs[0].get("pushpoptest").push("Z");
            lPINs[0].get("pushpoptest").push("a");
            return true; // Commit.
          },
          function()
          {
            lib_assert.deepEqual(lPINs[0].toPropValDict()["pushpoptest"], [1,2,3,4,5,6,"Z","a"], "pushtest after (mem)");
            lSS.next();
          });
      });
    lSS.push(function() { lPINs[0].refresh(lSS.simpleOnResponse); });
    lSS.push(function() { console.log(JSON.stringify(lPINs[0].toPropValDict())); lib_assert.deepEqual(lPINs[0].toPropValDict()["pushpoptest"], [1,2,3,4,5,6,"Z","a"], "pushtest after (disk)"); lSS.next(); });
    lSS.push(function() { console.log("done."); pOnSuccess(); });
    lSS.start();
  },
  test_shift_unshift:function(pOnSuccess)
  {
    var lSS = new InstrSeq();
    var lPID, lPINs;
    var lOnObjects = function(_pE, _pR) { var _lR = ""; lPID = _pR[0].pid; _pR.forEach(function(__pEl){ _lR += JSON.stringify(__pEl.toPropValDict()); }); lSS.simpleOnResponse(_pE, _lR);}
    lSS.push(function() { lMvStore.createPINs([{shifttest:[1,2,3,4,5,6,7,8]}], lOnObjects); });
    lSS.push(function() { lMvStore.mvsqlProto("SELECT * FROM @" + lPID.toString(16) + ";", function(_pE, _pR) { lPINs = _pR; lSS.simpleOnResponse(null, "found " + _pR.length + " results."); }); });
    lSS.push(
      function()
      {
        if (0 == lPINs.length) { lSS.next(); return; }
        lib_assert.deepEqual(lPINs[0].toPropValDict()["shifttest"], [1,2,3,4,5,6,7,8], "shifttest before");
        console.log("Performing 2 shifts.");
        doSimpleTx(
          function()
          {
            lPINs[0].get("shifttest").shift();
            lPINs[0].get("shifttest").shift();
            return true; // Commit.
          },
          function()
          {
            lib_assert.deepEqual(lPINs[0].toPropValDict()["shifttest"], [3,4,5,6,7,8], "shifttest after (mem)");
            lSS.next();
          });
      });
    lSS.push(function() { lPINs[0].refresh(lSS.simpleOnResponse); });
    lSS.push(function() { lib_assert.deepEqual(lPINs[0].toPropValDict()["shifttest"], [3,4,5,6,7,8], "shifttest after (disk)"); lSS.next(); });
    lSS.push(
      function()
      {
        if (0 == lPINs.length) { lSS.next(); return; }
        lib_assert.deepEqual(lPINs[0].toPropValDict()["shifttest"], [3,4,5,6,7,8], "unshifttest before");
        console.log("Performing 3 unshifts.");
        doSimpleTx(
          function()
          {
            lPINs[0].get("shifttest").unshift("a", "b");
            lPINs[0].get("shifttest").unshift(9, 10);
            lPINs[0].get("shifttest").unshift("c", 11);
            return true; // Commit.
          },
          function()
          {
            lib_assert.deepEqual(lPINs[0].toPropValDict()["shifttest"], ["c",11,9,10,"a","b",3,4,5,6,7,8], "unshifttest after (mem)");
            lSS.next();
          });
      });
    lSS.push(function() { lPINs[0].refresh(lSS.simpleOnResponse); });
    lSS.push(function() { console.log(JSON.stringify(lPINs[0].toPropValDict())); lib_assert.deepEqual(lPINs[0].toPropValDict()["shifttest"], ["c",11,9,10,"a","b",3,4,5,6,7,8], "unshifttest after (disk)"); lSS.next(); });
    lSS.push(function() { console.log("done."); pOnSuccess(); });
    lSS.start();
  },
  test_splice:function(pOnSuccess)
  {
    var lSS = new InstrSeq();
    var lPID, lPINs;
    var lOnObjects = function(_pE, _pR) { var _lR = ""; lPID = _pR[0].pid; _pR.forEach(function(__pEl){ _lR += JSON.stringify(__pEl.toPropValDict()); }); lSS.simpleOnResponse(_pE, _lR);}
    lSS.push(function() { lMvStore.createPINs([{splicetest:['aa','bb','cc','dd','ee','ff']}], lOnObjects); });
    lSS.push(function() { lMvStore.mvsqlProto("SELECT * FROM @" + lPID.toString(16) + ";", function(_pE, _pR) { lPINs = _pR; lSS.simpleOnResponse(null, "found " + _pR.length + " results."); }); });
    lSS.push(
      function()
      {
        if (0 == lPINs.length) { lSS.next(); return; }
        lib_assert.deepEqual(lPINs[0].toPropValDict()["splicetest"], ["aa","bb","cc","dd","ee","ff"], "splicetest before");
        lPINs[0].get("splicetest").splice(
          1,10,6,7,8,9,10,
          {txend:function() { lib_assert.deepEqual(lPINs[0].toPropValDict()["splicetest"], ["aa",6,7,8,9,10], "splicetest after (mem)"); lSS.next(); }});
      });
    lSS.push(function() { lPINs[0].refresh(lSS.simpleOnResponse); });
    lSS.push(function() { console.log(JSON.stringify(lPINs[0].toPropValDict())); lib_assert.deepEqual(lPINs[0].toPropValDict()["splicetest"], ["aa",6,7,8,9,10], "splicetest after (disk)"); lSS.next(); });
    lSS.push(function() { console.log("done."); pOnSuccess(); });
    lSS.start();
  },
  test_reverse:function(pOnSuccess)
  {
    var lSS = new InstrSeq();
    var lPID, lPINs;
    var lOnObjects = function(_pE, _pR) { var _lR = ""; lPID = _pR[0].pid; _pR.forEach(function(__pEl){ _lR += JSON.stringify(__pEl.toPropValDict()); }); lSS.simpleOnResponse(_pE, _lR);}
    lSS.push(function() { lMvStore.createPINs([{reversetest:["a","b","c","d","e","f","g"]}], lOnObjects); });
    lSS.push(function() { lMvStore.mvsqlProto("SELECT * FROM @" + lPID.toString(16) + ";", function(_pE, _pR) { lPINs = _pR; lSS.simpleOnResponse(null, "found " + _pR.length + " results."); }); });
    lSS.push(
      function()
      {
        if (0 == lPINs.length) { lSS.next(); return; }
        lib_assert.deepEqual(lPINs[0].toPropValDict()["reversetest"], ["a","b","c","d","e","f","g"], "reversetest before");
        lPINs[0].get("reversetest").reverse(
          {txend:function() { lib_assert.deepEqual(lPINs[0].toPropValDict()["reversetest"], ["g","f","e","d","c","b","a"], "reversetest after (mem)"); lSS.next(); }});
      });
    lSS.push(function() { lPINs[0].refresh(lSS.simpleOnResponse); });
    lSS.push(function() { console.log(JSON.stringify(lPINs[0].toPropValDict())); lib_assert.deepEqual(lPINs[0].toPropValDict()["reversetest"], ["g","f","e","d","c","b","a"], "reversetest after (disk)"); lSS.next(); });
    lSS.push(function() { console.log("done."); pOnSuccess(); });
    lSS.start();
  },
  test_sort:function(pOnSuccess)
  {
    var lSS = new InstrSeq();
    var lPID, lPINs;
    var lOnObjects = function(_pE, _pR) { var _lR = ""; lPID = _pR[0].pid; _pR.forEach(function(__pEl){ _lR += JSON.stringify(__pEl.toPropValDict()); }); lSS.simpleOnResponse(_pE, _lR);}
    lSS.push(function() { lMvStore.createPINs([{sorttest:[5,8,2,3,1,4,7,6]}], lOnObjects); });
    lSS.push(function() { lMvStore.mvsqlProto("SELECT * FROM @" + lPID.toString(16) + ";", function(_pE, _pR) { lPINs = _pR; lSS.simpleOnResponse(null, "found " + _pR.length + " results."); }); });
    lSS.push(
      function()
      {
        if (0 == lPINs.length) { lSS.next(); return; }
        lib_assert.deepEqual(lPINs[0].toPropValDict()["sorttest"], [5,8,2,3,1,4,7,6], "sorttest before");
        lPINs[0].get("sorttest").sort(
          {txend:function() { lib_assert.deepEqual(lPINs[0].toPropValDict()["sorttest"], [1,2,3,4,5,6,7,8], "sorttest after (mem)"); lSS.next(); }});
      });
    lSS.push(function() { lPINs[0].refresh(lSS.simpleOnResponse); });
    lSS.push(function() { console.log(JSON.stringify(lPINs[0].toPropValDict())); lib_assert.deepEqual(lPINs[0].toPropValDict()["sorttest"], [1,2,3,4,5,6,7,8], "sorttest after (disk)"); lSS.next(); });
    lSS.push(function() { console.log("done."); pOnSuccess(); });
    lSS.start();
  },
  
  /**
   * Simple tests for transactions.
   * There are 2 methods for controlling transactions:
   *   1. via mvsql statements; this requires a keep-alive connection (see lib_mvstore.createConnection)
   *   2. via the connection's startTx/commitTx/rollbackTx public methods, in protobuf mode
   */
  
  test_tx1:function(pOnSuccess)
  {
    var lSS = new InstrSeq();
    var lPID, lPINs, lCondPIN;
    lSS.push(function() { lMvStore.mvsql("INSERT (txtest) VALUES (5);", function(_pE, _pR) { lPID = _pR[0].id; lSS.next() }); });
    lSS.push(function() { lMvStore.mvsql("INSERT (txtest) VALUES (125);", lSS.next); });
    lSS.push(function() { lMvStore.mvsql("SELECT * WHERE (txtest > 100);", lSS.simpleOnResponse); });
    lSS.push(function() { lMvStore.mvsqlProto("SELECT * FROM @" + lPID.toString(16) + ";", function(_pE, _pR) { lPINs = _pR; lSS.simpleOnResponse(null, "found " + _pR.length + " results."); }); });
    lSS.push(
      function()
      {
        if (0 == lPINs.length) { lSS.next(); return; }
        lMvStore.startTx("main");
        lMvStore.startTx("property changes");
        lPINs[0].set("txtest", 6);
        lPINs[0].set("someotherprop", 6);
        lMvStore.commitTx(lSS.next);
      });
    lSS.push(
      function()
      {
        lMvStore.mvsqlProto("SELECT * WHERE txtest > 100;", function(_pE, _pR) { console.log("newselect: " + JSON.stringify(_pR)); if (_pR.length > 0 && "pid" in _pR[0]) lCondPIN = _pR[0]; lSS.next(); });
      });
    lSS.push(
      function()
      {
        if (undefined != lCondPIN)
          lPINs[0].set("conditionalprop", 6);
        lMvStore.commitTx(lSS.next);
      });
    lSS.push(function() { console.log("done."); pOnSuccess(); });
    lSS.start();
  },
  test_tx2:function(pOnSuccess)
  {
    var lSS = new InstrSeq();
    var lPID, lPINs, lCondPIN;
    lSS.push(function() { lMvStore.mvsql("INSERT (txtest) VALUES (5);", function(_pE, _pR) { lPID = _pR[0].id; lSS.next() }); });
    lSS.push(function() { lMvStore.mvsql("INSERT (txtest) VALUES (125);", lSS.next); });
    lSS.push(function() { lMvStore.mvsql("SELECT * WHERE (txtest > 100);", lSS.simpleOnResponse); });
    lSS.push(function() { lMvStore.mvsqlProto("SELECT * FROM @" + lPID.toString(16) + ";", function(_pE, _pR) { lPINs = _pR; lSS.simpleOnResponse(null, "found " + _pR.length + " results."); }); });
    lSS.push(
      function()
      {
        if (0 == lPINs.length) { lSS.next(); return; }
        lMvStore.startTx("main");
        lPINs[0].set("txtest", 6);
        lPINs[0].set("someotherprop", 6);
        lMvStore.mvsqlProto("SELECT * WHERE (txtest > 100);", function(_pE, _pR) { console.log("newselect: " + JSON.stringify(_pR)); if (_pR.length > 0 && "pid" in _pR[0]) lCondPIN = _pR[0]; lSS.next(); });
        // Note:
        //   Unlike in test_tx1, here the results returned by mvstore are mixed (prop sets + select)...
      });
    lSS.push(
      function()
      {
        if (undefined != lCondPIN)
          lPINs[0].set("conditionalprop", 6);
        lMvStore.commitTx(lSS.next);
      });
    lSS.push(function() { console.log("done."); pOnSuccess(); });
    lSS.start();
  },
  test_tx_simple_write:function(pOnSuccess)
  {
    var lSS = new InstrSeq();
    lSS.push(
      function()
      {
        lMvStore.startTx();
        lMvStore.createPINs(
          [{test_tx_simple_write:"whatever"}],
          function(_pE, _pR)
          {
            console.log("newpin " + JSON.stringify(_pR));
            lMvStore.commitTx(lSS.next);
          });
      });
    lSS.push(function() { console.log("done."); pOnSuccess(); });
    lSS.start();
  },
  test_tx_simple_read:function(pOnSuccess)
  {
    var lSS = new InstrSeq();
    lSS.push(function() { lMvStore.mvsql("INSERT (txtest) VALUES (125);", lSS.next); });
    lSS.push(
      function()
      {
        lMvStore.startTx();
        lMvStore.mvsqlProto(
          "SELECT * WHERE (txtest > 100);",
          function(_pE, _pR)
          {
            console.log("newselect: " + JSON.stringify(_pR));
            lMvStore.commitTx(lSS.next);
          });
      });
    lSS.push(function() { console.log("done."); pOnSuccess(); });
    lSS.start();
  },
  test_tx_relying_on_keep_alive:function(pOnSuccess)
  {
    if (!lMvStore.keptAlive())
    {
      console.log("warning: this test requires a keep-alive connection - skipped.");
      pOnSuccess();
      return;
    }
    var lSS = new InstrSeq();
    var lPID;
    lSS.push(function() { console.log("Creating an object and committing."); lMvStore.mvsql("START TRANSACTION;", lSS.simpleOnResponse); });
    lSS.push(function() { lMvStore.mvsql("INSERT (tx_keepalive_committed) VALUES (1);", function(_pE, _pR) { lPID = _pR[0].id; lSS.next() }); });
    lSS.push(function() { lMvStore.mvsql("COMMIT;", lSS.simpleOnResponse); });
    lSS.push(function() { console.log("Adding a property and rolling back."); lMvStore.mvsql("START TRANSACTION;", lSS.simpleOnResponse); });
    lSS.push(function() { lMvStore.mvsql("UPDATE @" + lPID.toString(16) + " SET tx_keepalive_rolledback=2;", lSS.simpleOnResponse); });
    lSS.push(function() { lMvStore.mvsql("SELECT * FROM @" + lPID.toString(16) + ";", function(_pE, _pR) { lib_assert.ok("tx_keepalive_committed" in _pR[0] && "tx_keepalive_rolledback" in _pR[0], "before rollback"); lSS.next(); }); });
    lSS.push(function() { lMvStore.mvsql("ROLLBACK;", lSS.simpleOnResponse); });
    lSS.push(function() { lMvStore.mvsql("SELECT * FROM @" + lPID.toString(16) + ";", function(_pE, _pR) { lib_assert.ok("tx_keepalive_committed" in _pR[0] && !("tx_keepalive_rolledback" in _pR[0]), "after rollback"); lSS.next(); }); });
    lSS.push(function() { console.log("done."); pOnSuccess(); });
    lSS.start();
  },

  /**
   * Other unit tests.
   */

  test_types:function(pOnSuccess) // Note: Very similar to testtypes1.py (in python)...
  {
    var lSS = new InstrSeq();
    var lStdTest =
      function(_pValue, _pValueStr, _pVT, _pComment)
      {
        lSS.push(
          function()
          {
            var _lSS = new InstrSeq();
            var _lPIN;
            _lSS.push(function() { lMvStore.mvsqlProto("INSERT (\"http://localhost/mv/property/test_types/value1\") VALUES (" + _pValueStr + ");", function(_pE, _pR) { assertValidResult(_pR); _lPIN = _pR[0]; _lSS.next() }); });
            _lSS.push(function() { _lPIN.set("http://localhost/mv/property/test_types/value2", _pValue, {txend:function(){_lSS.next();}}); });
            _lSS.push(function() { _lPIN.refresh(_lSS.next); });
            _lSS.push(
              function()
              {
                var _lCmpEval = function(__pV) { return (typeof(__pV) == "object") ? __pV.toString() : __pV; } // Note: In javascript comparison of objects is always by reference by default...
                lib_assert.ok(_lCmpEval(_pValue) == _lCmpEval(_lPIN.get("http://localhost/mv/property/test_types/value1")));
                lib_assert.ok(_lCmpEval(_pValue) == _lCmpEval(_lPIN.get("http://localhost/mv/property/test_types/value2")));
                lib_assert.ok(_pVT == _lPIN.getExtras()["http://localhost/mv/property/test_types/value1"].type);
                lib_assert.ok(_pVT == _lPIN.getExtras()["http://localhost/mv/property/test_types/value2"].type);
                console.log(_pVT + " passed" + (_pComment ? _pComment : ""));
                _lSS.next();
              });
            _lSS.push(lSS.next);
            _lSS.start();
          });
      }

    /**
     * Standard/native javascript types.
     */

    // VT_STRING
    var _lValue_VT_STRING = "Hello how are you";
    lStdTest(_lValue_VT_STRING, "'" + _lValue_VT_STRING + "'", 'VT_STRING');

    // VT_DOUBLE
    var _lValues_VT_DOUBLE = [123.456, 2.2e-308, 1.5e+308, -1.5e-50/*, 0*/];
    for (var _iV = 0; _iV < _lValues_VT_DOUBLE.length; _iV++)
      { lStdTest(_lValues_VT_DOUBLE[_iV], _lValues_VT_DOUBLE[_iV], 'VT_DOUBLE', " (" + _lValues_VT_DOUBLE[_iV] + ")"); }

    // VT_BOOL
    lStdTest(true, true.toString(), 'VT_BOOL', " (true)");
    lStdTest(false, false.toString(), 'VT_BOOL', " (false)");

    // VT_DATETIME
    // TODO: VT_INTERVAL
    var _lValues_VT_DATETIME = new Date();
    var _l2Digits = function(_pNum) { return (_pNum < 10 ? "0" : "") + _pNum; }
    var _lDate2Str = function(_pDate) { return _pDate.getUTCFullYear().toString() + "-" + _l2Digits(1 + _pDate.getUTCMonth()) + "-" + _l2Digits(_pDate.getUTCDate()) + " " + _l2Digits(_pDate.getUTCHours()) + ":" + _l2Digits(_pDate.getUTCMinutes()) + ":" + _l2Digits(_pDate.getUTCSeconds()); }
    lStdTest(_lValues_VT_DATETIME, "TIMESTAMP'" + _lDate2Str(_lValues_VT_DATETIME) + "'", 'VT_DATETIME');

    /**
     * Exotic types.
     */

    // VT_BSTR
    lSS.push(
      function()
      {
        var _lSS = new InstrSeq();
        var _lPIN;
        var _lValue = new Buffer("Hello how are you", "binary");
        var _lBin2HexStr = function(_pBin)
        {
          var __lStr = "";
          for (var __iB = 0; __iB < _pBin.length; __iB++)
          {
            var __lC = _pBin[__iB].toString(16);
            __lStr += (__lC.length < 2 ? "0" : "") + __lC;
          }
          return __lStr;
        }
        var _lCompareBuffers = function(_pBuf1, _pBuf2)
        {
          var __lLenDiff = _pBuf1.length - _pBuf2.length;
          if (0 != __lLenDiff)
            return __lLenDiff;
          for (var __iB = 0; __iB < _pBuf1.length; __iB++)
          {
            var __lDiff = _pBuf1[__iB] - _pBuf2[__iB];
            if (0 != __lDiff)
              return __lDiff;
          }
          return 0;
        }
        _lSS.push(function() { lMvStore.mvsqlProto("INSERT (\"http://localhost/mv/property/test_types/value1\") VALUES (X'" + _lBin2HexStr(_lValue) + "');", function(_pE, _pR) { assertValidResult(_pR); _lPIN = _pR[0]; _lSS.next() }); });
        _lSS.push(function() { _lPIN.set("http://localhost/mv/property/test_types/value2", _lValue, {txend:function(){_lSS.next();}}); });
        _lSS.push(function() { _lPIN.refresh(_lSS.next); });
        _lSS.push(
          function()
          {
            lib_assert.ok(_lPIN.get("http://localhost/mv/property/test_types/value1") instanceof process.binding('buffer').SlowBuffer);
            lib_assert.ok(_lPIN.get("http://localhost/mv/property/test_types/value2") instanceof process.binding('buffer').SlowBuffer);
            lib_assert.ok(0 == _lCompareBuffers(_lValue, _lPIN.get("http://localhost/mv/property/test_types/value1")));
            lib_assert.ok(0 == _lCompareBuffers(_lValue, _lPIN.get("http://localhost/mv/property/test_types/value2")));
            lib_assert.ok('VT_BSTR' == _lPIN.getExtras()["http://localhost/mv/property/test_types/value1"].type);
            lib_assert.ok('VT_BSTR' == _lPIN.getExtras()["http://localhost/mv/property/test_types/value2"].type);
            console.log("VT_BSTR passed");
            _lSS.next();
          });
        _lSS.push(lSS.next);
        _lSS.start();
      });

    // VT_URL
    lSS.push(
      function()
      {
        var _lSS = new InstrSeq();
        var _lPIN;
        var _lValue = lMvStore.makeUrl("urn:issn:1234-5678");
        _lSS.push(function() { lMvStore.mvsqlProto("INSERT (\"http://localhost/mv/property/test_types/value1\") VALUES (U'" + _lValue.toString() + "');", function(_pE, _pR) { assertValidResult(_pR); _lPIN = _pR[0]; _lSS.next() }); });
        _lSS.push(function() { _lPIN.set("http://localhost/mv/property/test_types/value2", _lValue, {txend:function(){_lSS.next();}}); });
        _lSS.push(function() { _lPIN.refresh(_lSS.next); });
        _lSS.push(
          function()
          {
            lib_assert.ok(_lPIN.get("http://localhost/mv/property/test_types/value1").isUrl());
            lib_assert.ok(_lPIN.get("http://localhost/mv/property/test_types/value2").isUrl());
            lib_assert.ok(_lValue == _lPIN.get("http://localhost/mv/property/test_types/value1").toString());
            lib_assert.ok(_lValue == _lPIN.get("http://localhost/mv/property/test_types/value2").toString());
            lib_assert.ok('VT_URL' == _lPIN.getExtras()["http://localhost/mv/property/test_types/value1"].type);
            lib_assert.ok('VT_URL' == _lPIN.getExtras()["http://localhost/mv/property/test_types/value2"].type);
            console.log("VT_URL passed");
            _lSS.next();
          });
        _lSS.push(lSS.next);
        _lSS.start();
      });

    // VT_INT/VT_UINT/VT_INT64/VT_UINT64/VT_FLOAT
    // Note:
    //   At the moment our node.js library does not allow to specify these types via protobuf
    //   (because they're not native js numeric types); here we only verify that the original
    //   type (in the store) remains accessible (e.g. for compatibility with non-js apps).
    lSS.push(
      function()
      {
        var _lSS = new InstrSeq();
        var _lPIN;
        var _lValue = 123.456;
        _lSS.push(function() { lMvStore.mvsqlProto("INSERT (\"http://localhost/mv/property/test_types/value1\") VALUES (" + _lValue + "f);", function(_pE, _pR) { assertValidResult(_pR); _lPIN = _pR[0]; _lSS.next() }); });
        _lSS.push(
          function()
          {
            lib_assert.ok(_lValue - 0.00000001 < _lPIN.get("http://localhost/mv/property/test_types/value1") < _lValue + 0.00000001);
            lib_assert.ok(typeof(_lPIN.get("http://localhost/mv/property/test_types/value1")) == "number");
            lib_assert.ok('VT_FLOAT' == _lPIN.getExtras()["http://localhost/mv/property/test_types/value1"].type);
            console.log("VT_INT/VT_UINT/VT_INT64/VT_UINT64/VT_FLOAT passed");
            _lSS.next();
          });
        _lSS.push(lSS.next);
        _lSS.start();
      });      

    // VT_REFID/VT_REFIDPROP/VT_REFIDELT
    lSS.push(
      function()
      {
        var _lSS = new InstrSeq();
        var _lPINReferenced;
        _lSS.push(function() { lMvStore.createPINs([{"http://localhost/mv/property/test_types/bogus":[1,2,3,4]}], function(_pE, _pR) { assertValidResult(_pR); _lPINReferenced = _pR[0]; _lSS.next() }); });
        _lSS.push(function() { _lPINReferenced.refresh(_lSS.next); }); // TODO: Check why I need to do this... shouldn't...
        var _lRefid;
        var _lRefidprop;
        var _lRefidelt;
        _lSS.push(
          function()
          {
            _lRefid = lMvStore.makeRef(_lPINReferenced.pid);
            _lRefidprop = lMvStore.makeRef(_lPINReferenced.pid, 0, "http://localhost/mv/property/test_types/bogus");
            _lRefidelt = lMvStore.makeRef(_lPINReferenced.pid, 0, "http://localhost/mv/property/test_types/bogus", _lPINReferenced.getExtras()["http://localhost/mv/property/test_types/bogus"][2].eid); // TODO: don't use getExtras for this, offer a better way.
            _lSS.next();
          });
        var _lPINReferencing;
        _lSS.push(function() { lMvStore.mvsqlProto("INSERT (\"http://localhost/mv/property/test_types/REFID/value1\", \"http://localhost/mv/property/test_types/REFIDPROP/value1\", \"http://localhost/mv/property/test_types/REFIDELT/value1\") VALUES (" + _lRefid.toString() + "," + _lRefidprop.toString() + "," + _lRefidelt.toString() + ");", function(_pE, _pR) { assertValidResult(_pR); _lPINReferencing = _pR[0]; _lSS.next() }); });
        _lSS.push(function() { _lPINReferencing.set("http://localhost/mv/property/test_types/REFID/value2", _lRefid, {txend:function(){_lSS.next();}}); });
        _lSS.push(function() { _lPINReferencing.set("http://localhost/mv/property/test_types/REFIDPROP/value2", _lRefidprop, {txend:function(){_lSS.next();}}); });
        _lSS.push(function() { _lPINReferencing.set("http://localhost/mv/property/test_types/REFIDELT/value2", _lRefidelt, {txend:function(){_lSS.next();}}); });
        _lSS.push(function() { _lPINReferencing.refresh(_lSS.next); });
        _lSS.push(
          function()
          {
            lib_assert.ok(_lPINReferencing.get("http://localhost/mv/property/test_types/REFID/value1").isRef());
            lib_assert.ok(_lPINReferencing.get("http://localhost/mv/property/test_types/REFID/value2").isRef());
            lib_assert.ok(_lPINReferencing.get("http://localhost/mv/property/test_types/REFIDPROP/value1").isRef());
            lib_assert.ok(_lPINReferencing.get("http://localhost/mv/property/test_types/REFIDPROP/value2").isRef());
            lib_assert.ok(_lPINReferencing.get("http://localhost/mv/property/test_types/REFIDELT/value1").isRef());
            lib_assert.ok(_lPINReferencing.get("http://localhost/mv/property/test_types/REFIDELT/value2").isRef());
            lib_assert.ok(_lRefid.toString() == _lPINReferencing.get("http://localhost/mv/property/test_types/REFID/value1").toString());
            lib_assert.ok(_lRefid.toString() == _lPINReferencing.get("http://localhost/mv/property/test_types/REFID/value2").toString());
            lib_assert.ok(_lRefidprop.toString() == _lPINReferencing.get("http://localhost/mv/property/test_types/REFIDPROP/value1").toString());            
            lib_assert.ok(_lRefidprop.toString() == _lPINReferencing.get("http://localhost/mv/property/test_types/REFIDPROP/value2").toString());
            lib_assert.ok(_lRefidelt.toString() == _lPINReferencing.get("http://localhost/mv/property/test_types/REFIDELT/value1").toString());
            lib_assert.ok(_lRefidelt.toString() == _lPINReferencing.get("http://localhost/mv/property/test_types/REFIDELT/value2").toString());
            lib_assert.ok('VT_REFID' == _lPINReferencing.getExtras()["http://localhost/mv/property/test_types/REFID/value1"].type);
            lib_assert.ok('VT_REFID' == _lPINReferencing.getExtras()["http://localhost/mv/property/test_types/REFID/value2"].type);
            lib_assert.ok('VT_REFIDPROP' == _lPINReferencing.getExtras()["http://localhost/mv/property/test_types/REFIDPROP/value1"].type);
            lib_assert.ok('VT_REFIDPROP' == _lPINReferencing.getExtras()["http://localhost/mv/property/test_types/REFIDPROP/value2"].type);
            lib_assert.ok('VT_REFIDELT' == _lPINReferencing.getExtras()["http://localhost/mv/property/test_types/REFIDELT/value1"].type);
            lib_assert.ok('VT_REFIDELT' == _lPINReferencing.getExtras()["http://localhost/mv/property/test_types/REFIDELT/value2"].type);
            console.log("VT_REFID/VT_REFIDPROP/VT_REFIDELT passed");
            _lSS.next();
          });
        _lSS.push(lSS.next);
        _lSS.start();
      });

    lSS.push(function() { console.log("done."); pOnSuccess(); });
    lSS.start();
  },
  test_qnames:function(pOnSuccess)
  {
    if (!lMvStore.keptAlive())
    {
      console.log("warning: this test requires a keep-alive connection - skipped.");
      pOnSuccess();
      return;
    }
    var lSS = new InstrSeq();
    var lPID;
    lSS.push(function() { lMvStore.mvsql("SET PREFIX myqnamec: 'http://localhost/mv/class/test_qnames/';", lSS.next); });
    lSS.push(function() { lMvStore.mvsql("SET PREFIX myqnamep: 'http://localhost/mv/property/test_qnames/';", lSS.next); });
    var lClassesExist = false;
    var lOnSelectClasses = function(pError, pResponse) { console.log("substep " + lSS.curstep()); if (pError) console.log("\n*** ERROR: " + pError + "\n"); else { console.log("Result from step " + lSS.curstep() + ":" + JSON.stringify(pResponse)); lClassesExist = (pResponse && pResponse.length > 0); lSS.next(); } }
    lSS.push(function() { lMvStore.mvsql("SELECT * FROM mv:ClassOfClasses WHERE BEGINS(mv:classID, 'http://localhost/mv/class/test_qnames/');", lOnSelectClasses); });
    lSS.push(function() { if (lClassesExist) lSS.next(); else lMvStore.mvsql("CREATE CLASS myqnamec:pos AS SELECT * WHERE EXISTS(myqnamep:x) AND EXISTS(myqnamep:y);", lSS.simpleOnResponse); });
    lSS.push(function() { lMvStore.mvsql("INSERT (myqnamep:x, myqnamep:y) VALUES (" + Math.random() + "," + Math.random() + ");", lSS.simpleOnResponse); });
    lSS.push(function() { lMvStore.mvsqlProto("SELECT * FROM myqnamec:pos;", function(_pE, _pR) { assertValidResult(_pR); for (var _iP = 0; _iP < _pR.length; _iP++) { console.log(JSON.stringify(_pR[_iP].toPropValDict())); } lSS.next(); }); });
    lSS.push(function() { console.log("done."); pOnSuccess(); });
    lSS.start();
  },
  test_scalar_vs_collection:function(pOnSuccess)
  {
    var lSS = new InstrSeq();
    var lPIN;
    var lOnObjects = function(_pE, _pR) { assertValidResult(_pR); lPIN = _pR[0]; lSS.next(); }
    lSS.push(function() { lMvStore.createPINs([{test_scalar_vs_collection:"whatever"}], lOnObjects); });
    lSS.push(function() { lPIN.set("test_scalar_vs_collection", ["s1", 2, "s3", 4], {txend:lSS.next}); });
    lSS.push(function() { lib_assert.deepEqual(lPIN.toPropValDict()["test_scalar_vs_collection"], ["s1", 2, "s3", 4], "scalar -> collection (1)"); lPIN.refresh(lSS.next); });
    lSS.push(function() { lib_assert.deepEqual(lPIN.toPropValDict()["test_scalar_vs_collection"], ["s1", 2, "s3", 4], "scalar -> collection (2)"); lSS.next(); });
    lSS.push(function() { lPIN.set("test_scalar_vs_collection", [1, "s2", 3], {txend:lSS.next}); });
    lSS.push(function() { lib_assert.deepEqual(lPIN.toPropValDict()["test_scalar_vs_collection"], [1, "s2", 3], "collection -> collection (1)"); lPIN.refresh(lSS.next); });
    lSS.push(function() { lib_assert.deepEqual(lPIN.toPropValDict()["test_scalar_vs_collection"], [1, "s2", 3], "collection -> collection (2)"); lSS.next(); });
    lSS.push(function() { lPIN.set("test_scalar_vs_collection", 123, {txend:lSS.next}); });
    lSS.push(function() { lib_assert.deepEqual(lPIN.toPropValDict()["test_scalar_vs_collection"], 123, "collection -> scalar (1)"); lPIN.refresh(lSS.next); });
    lSS.push(function() { lib_assert.deepEqual(lPIN.toPropValDict()["test_scalar_vs_collection"], 123, "collection -> scalar (2)"); lSS.next(); });
    lSS.push(function() { console.log("done."); pOnSuccess(); });
    lSS.start();
  },

  /**
   * Simple pseudo-application: personal photo server storage back-end.
   *
   * This example relies exclusively on the mvsql mode with json responses (no protobuf).
   * The transaction-control statements are disabled if the connection is not using keep-alive.
   * The data model is essentially relational (no collection or reference, just classes).
   *
   * This application simulates a personal photo server, containing records for actual photos,
   * along with a registry of guests, each being granted individual and group privileges to
   * see some of the photos. The application does the following:
   *
   *   1. it declares a few classes (if not already present, from previous runs)
   *   2. it removes any old data from previous runs
   *   3. it creates records for fake "photos" (it walks the kernel tests directory and interprets any cpp file as if it were a photo file)
   *   4. it creates a bunch of users and groups
   *   5. it grants access to photos, to specific users and groups (randomly)
   *   6. it counts how many photos a specific guest can view
   *   7. it double-checks everything with an in-memory representation
   */

  test_app_photos1:function(pOnSuccess) // Same as python/tests/testPhotos1.py. Uses only mvsql and mvsqlCount.
  {
    if (!lMvStore.keptAlive())
    {
      console.log("warning: transactions in this test will not be perfectly respected without a keep-alive connection.");
    }
    var lSS = new InstrSeq();
    var lChkCount = function(_pName, _pExpected, _pActual) { console.log((_pExpected == _pActual ? "YEAH" : "\n***\n*** WARNING") + ": expected " + _pExpected + " " + _pName + ", found " + _pActual + "."); lib_assert.ok(_pExpected == _pActual); }
    var lWalkDir = function(_pDir, _pExt) // Returns an array of all files with extension _pExt under _pDir (recursively; synchronous[!]).
    {
      var __lResult = new Array();
      var __lRegexp = new RegExp(_pExt + "$");
      var __lProcessFN = function(__pFN) { var ___lFFN = _pDir + "/" + __pFN; if (lib_fs.statSync(___lFFN).isFile()) { if (-1 != ___lFFN.search(__lRegexp)) { console.log("file: " + ___lFFN); __lResult.push({dirname:_pDir, filename:__pFN}); } } else __lResult = __lResult.concat(lWalkDir(___lFFN, _pExt)); }
      var __lFN = lib_fs.readdirSync(_pDir);
      __lFN.forEach(__lProcessFN);  
      return __lResult;
    }
    function InMemoryChk() // Performs a side-by-side in-memory comparison, to validate every step of the test.
    {
      this.mPhotos = {};
      this.mUsers = {};
      this.mGroups = {};
      this.tagPhoto = function(pPhoto, pTag) { if (!(pPhoto in this.mPhotos)) this.mPhotos[pPhoto] = {}; this.mPhotos[pPhoto][pTag] = 1; };
      this.setUserGroup = function(pUser, pGroup) { if (!(pUser in this.mUsers)) this.mUsers[pUser] = {tags:{}}; if (!(pGroup in this.mGroups)) this.mGroups[pGroup] = {}; this.mUsers[pUser]["group"] = pGroup; };
      this.addUserPrivilege = function(pUser, pTag) { if (!(pUser in this.mUsers)) this.mUsers[pUser] = {tags:{}}; this.mUsers[pUser]["tags"][pTag] = 1; };
      this.addGroupPrivilege = function(pGroup, pTag) { if (!(pGroup in this.mGroups)) this.mGroups[pGroup] = {}; this.mGroups[pGroup][pTag] = 1; };
      this.getTags_usPriv = function(pUser) { if (!(pUser in this.mUsers)) return 0; return Object.keys(this.mUsers[pUser]["tags"]); };
      this.getTags_grPriv = function(pUser) { if (!(pUser in this.mUsers)) return 0; return Object.keys(this.mGroups[this.mUsers[pUser]["group"]]); };
      this.getUserTags = function(pUser) { var lTags = {}; this.getTags_usPriv(pUser).forEach(function(_pEl){ lTags[_pEl] = 1; }); this.getTags_grPriv(pUser).forEach(function(_pEl){ lTags[_pEl] = 1; }); return Object.keys(lTags); }
      this.countPhotos = function(pUser) { if (!(pUser in this.mUsers)) return 0; _lTags = this.getUserTags(pUser); _lPhotos = {}; for (var _iP in this.mPhotos) { for (_iT in this.mPhotos[_iP]) { if (_lTags.indexOf(_iT) >= 0) _lPhotos[_iP] = 1; } } return Object.keys(_lPhotos).length; };
      this.print = function() { console.log(JSON.stringify(this)); }
    };
    var lInMemoryChk = new InMemoryChk();
    var lRandomString = function(_pLen) // Produces a random string of the specified length (synchronous).
    {
      var _lResult = "";
      var _lChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
      for (var _i = 0; _i < _pLen; _i++)
        _lResult += _lChars.charAt(Math.floor(Math.random() * _lChars.length));
      return _lResult;
    }
    var lStartTx = function(_pSS) { if (lMvStore.keptAlive()) { lMvStore.mvsql("START TRANSACTION;", _pSS.next); } else { _pSS.next(); } }
    var lCommitTx = function(_pSS) { if (lMvStore.keptAlive()) { lMvStore.mvsql("COMMIT;", _pSS.next); } else { _pSS.next(); } }
    var lCreatePhoto = function(_pDir, _pFileName, _pOnSuccess) // Creates the specified photo object in the db (asynchronous).
    {
      var _lFullPath = _pDir + "/" + _pFileName;
      console.log("adding file " + _lFullPath);
      var _lDate = lib_fs.statSync(_lFullPath).ctime;
      var _lHash = "";
      var _lSS = new InstrSeq();
      _lSS.push(function() { lib_sys.exec("uuidgen", function(__pErr, __pStdout, __pStderr) { _lHash = __pStdout; _lSS.next(); }); });
      _lSS.push(
        function()
        {
          var __lDateStr = _lDate.toJSON().substring(0, 10);
          var __lTimeStr = _lDate.toLocaleTimeString();
          lMvStore.mvsql("INSERT (\"http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#hasHash\", \"http://www.w3.org/2001/XMLSchema#date\", \"http://www.w3.org/2001/XMLSchema#time\", \"http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#fileUrl\", \"http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#fileName\") VALUES ('" + _lHash + "', TIMESTAMP'" + __lDateStr + "', INTERVAL'" + __lTimeStr + "', '" + _pDir + "', '" + _pFileName + "');", function(__pE, __pR) { assertValidResult(__pR); _lSS.next() });
        });
      _lSS.push(_pOnSuccess);
      _lSS.start();
    };
    var lCreateUser = function(_pUser, _pGroup, _pOnSuccess) // Creates the specified user object in the db (asynchronous).
    {
      lInMemoryChk.setUserGroup(_pUser, _pGroup);
      var _lSS = new InstrSeq();
      _lSS.push(function() { lMvStore.mvsql("INSERT (\"http://xmlns.com/foaf/0.1/mbox\", \"http://www.w3.org/2002/01/p3prdfv1#user.login.password\", \"http://xmlns.com/foaf/0.1/member/adomain:Group\") VALUES ('" + _pUser + "', '" + lRandomString(20)  + "', '" + _pGroup + "');", _lSS.next); });
      _lSS.push(function() { lMvStore.mvsqlCount("SELECT * FROM \"http://localhost/mv/class/testphotos1/user\" WHERE \"http://xmlns.com/foaf/0.1/member/adomain:Group\"='" + _pGroup + "';", function(__pE, __pR) { console.log("group " + _pGroup + " contains " + __pR + " users"); _lSS.next(); }) });
      _lSS.push(_pOnSuccess);
      _lSS.start();
    };
    var lSelectDistinctGroups = function(_pOnSuccess) // Selects all distinct group names (asynchronous).
    {
      // Review: eventually mvstore will allow to SELECT DISTINCT(groupid) FROM users...
      lMvStore.mvsql(
        "SELECT * FROM \"http://localhost/mv/class/testphotos1/user\";",
        function(__pE, __pR)
        {
          if (__pE) _pOnSuccess([]);
          var _lGroupIds = {};
          __pR.forEach(function(___pEl){ _lGroupIds[___pEl["http://xmlns.com/foaf/0.1/member/adomain:Group"]] = 1; });
          _pOnSuccess(Object.keys(_lGroupIds));
        });
    };
    var lAssignTagRandomly = function(_pTagName, _pOnSuccess) // Creates the specified tag and assigns it to a random selection of photos (asynchronous).
    {
      var _lTagCount = 0;
      var _lSS = new InstrSeq();
      var _lTagPhoto = function(__pPhoto, __pOnSuccess)
      {
        if (Math.random() > 0.10)
          __pOnSuccess();
        else
        {
          lInMemoryChk.tagPhoto(__pPhoto["http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#hasHash"], _pTagName);
          lMvStore.mvsql("INSERT (\"http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#hasHash\", \"http://code.google.com/p/tagont/hasTagLabel\") VALUES ('" + __pPhoto["http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#hasHash"] + "', '" + _pTagName + "');", __pOnSuccess);
        }
      };
      _lSS.push(function() { lStartTx(_lSS); });
      _lSS.push(function() { lMvStore.mvsqlCount("SELECT * FROM \"http://localhost/mv/class/testphotos1/tag\" WHERE \"http://code.google.com/p/tagont/hasTagLabel\"='" + _pTagName + "';", function(__pE, __pR) { _lTagCount = __pR; _lSS.next(); }); });
      _lSS.push(function() { if (0 == _lTagCount) { console.log("adding tag " + _pTagName); lMvStore.mvsql("INSERT (\"http://code.google.com/p/tagont/hasTagLabel\") VALUES ('" + _pTagName + "');", _lSS.next); } else _lSS.next(); });
      _lSS.push(function() { lMvStore.mvsql("SELECT * FROM \"http://localhost/mv/class/testphotos1/photo\";", function(__pE, __pR) { var __lSS = new InstrSeq(); __pR.forEach(function(___pEl) { __lSS.push(function() { _lTagPhoto(___pEl, __lSS.next) }); }); __lSS.push(_lSS.next); __lSS.start(); }); });
      _lSS.push(function() { lCommitTx(_lSS); });
      _lSS.push(_pOnSuccess);
      _lSS.start();
    };
    var lAssignGroupPrivilegesRandomly = function(_pOnSuccess) // Assigns a random selection of tags to each existing group (asynchronous).
    {
      var _lSS = new InstrSeq();
      var _lGroupIds = null, _lTags = null;
      var _lOneTag = function(__pGroupId, __pTagName, __pSS)
      {
        lInMemoryChk.addGroupPrivilege(__pGroupId, __pTagName);
        __pSS.push(function() { lMvStore.mvsql("INSERT (\"http://code.google.com/p/tagont/hasTagLabel\", \"http://code.google.com/p/tagont/hasVisibility\") VALUES ('" + __pTagName + "', '" + __pGroupId + "');", __pSS.next); });
      }
      var _lOneIter = function(__pGroupId, __pOnSuccess)
      {
        var __lSS = new InstrSeq(); 
        for (var __iT = 0; __iT < _lTags.length; __iT++)
        {
          if (Math.random() < 0.5)
            continue;
          var __lTagName = _lTags[__iT]["http://code.google.com/p/tagont/hasTagLabel"];
          _lOneTag(__pGroupId, __lTagName, __lSS);
        }
        __lSS.push(__pOnSuccess);
        __lSS.start();
      };
      _lSS.push(function() { lSelectDistinctGroups(function(__pR) { _lGroupIds = __pR.slice(0); console.log("groups: " + JSON.stringify(_lGroupIds)); _lSS.next(); }); });
      _lSS.push(function() { lMvStore.mvsql("SELECT * FROM \"http://localhost/mv/class/testphotos1/tag\";", function(__pE, __pR) { _lTags = __pR.slice(0); var __lTagsStr = ""; __pR.forEach(function(___pEl) { __lTagsStr = __lTagsStr + ___pEl["http://code.google.com/p/tagont/hasTagLabel"] + " "; }); console.log("tags: " + __lTagsStr); _lSS.next(); }); });
      _lSS.push(function() { var __lSS = new InstrSeq(); _lGroupIds.forEach(function(__pEl) { __lSS.push(function() { _lOneIter(__pEl, __lSS.next); }); }); __lSS.push(_lSS.next); __lSS.start(); });
      _lSS.push(_pOnSuccess);
      _lSS.start();
    }
    var lAssignUserPrivilegesRandomly = function(_pOnSuccess) // Assigns a random selection of tags to each existing user (asynchronous).
    {
      var _lSS = new InstrSeq();
      var _lUsers = null, _lTags = null;
      var _lOneTag = function(__pSS, __pUserName, __pTagName)
      {
        lInMemoryChk.addUserPrivilege(__pUserName, __pTagName);
        __pSS.push(function() { lMvStore.mvsql("INSERT (\"http://code.google.com/p/tagont/hasTagLabel\", \"http://code.google.com/p/tagont/hasVisibility\") VALUES ('" + __pTagName + "', '" + __pUserName + "');", __pSS.next); });
      }
      var _lOneIter = function(__pUser, __pOnSuccess)
      {
        var __lSS = new InstrSeq();
        var __lRange = Math.random();
        for (var __iT = 0; __iT < _lTags.length; __iT++)
        {
          if (Math.random() < __lRange)
            continue;
          var __lTagName = _lTags[__iT]["http://code.google.com/p/tagont/hasTagLabel"];
          _lOneTag(__lSS, __pUser["http://xmlns.com/foaf/0.1/mbox"], __lTagName);
        }
        __lSS.push(__pOnSuccess);
        __lSS.start();
      };
      _lSS.push(function() { lMvStore.mvsql("SELECT * FROM \"http://localhost/mv/class/testphotos1/user\";", function(__pE, __pR) { _lUsers = __pR.slice(0); var __lUsersStr = ""; __pR.forEach(function(___pEl) { __lUsersStr = __lUsersStr + ___pEl["http://xmlns.com/foaf/0.1/mbox"] + " "; }); console.log("users: " + __lUsersStr); _lSS.next(); }); });
      _lSS.push(function() { lMvStore.mvsql("SELECT * FROM \"http://localhost/mv/class/testphotos1/tag\";", function(__pE, __pR) { _lTags = __pR.slice(0); var __lTagsStr = ""; __pR.forEach(function(___pEl) { __lTagsStr = __lTagsStr + ___pEl["http://code.google.com/p/tagont/hasTagLabel"] + " "; }); console.log("tags: " + __lTagsStr); _lSS.next(); }); });
      _lSS.push(function() { var __lSS = new InstrSeq(); _lUsers.forEach(function(__pEl) { __lSS.push(function() { _lOneIter(__pEl, __lSS.next); }); }); __lSS.push(_lSS.next); __lSS.start(); });
      _lSS.push(_pOnSuccess);
      _lSS.start();
    };
    var lGetUsersOfInterest = function(_pTags, _pOnSuccess) // Find users who can see the first 5 of the specified tags (asynchronous).
    {
      var _lFirstTags = new Array(); _pTags.slice(0, 1).forEach(function(__pEl) { _lFirstTags.push("'" + __pEl["http://code.google.com/p/tagont/hasTagLabel"] + "'"); });
      lMvStore.mvsql(
        // TODO: Review this query once bug #202 is sorted out.
        //"SELECT * FROM \"http://localhost/mv/class/testphotos1/user\" AS u JOIN \"http://localhost/mv/class/testphotos1/privilege\" AS p ON (u.\"http://xmlns.com/foaf/0.1/mbox\" = p.\"http://code.google.com/p/tagont/hasVisibility\") WHERE p.\"http://code.google.com/p/tagont/hasTagLabel\" IN (" + _lFirstTags + ");",
        //"SELECT * FROM \"http://localhost/mv/class/testphotos1/privilege\" AS p JOIN \"http://localhost/mv/class/testphotos1/user\" AS u ON (p.\"http://code.google.com/p/tagont/hasVisibility\" = u.\"http://xmlns.com/foaf/0.1/mbox\") WHERE p.\"http://code.google.com/p/tagont/hasTagLabel\" IN (" + _lFirstTags + ");",
        "SELECT * FROM \"http://localhost/mv/class/testphotos1/privilege\"(" + _lFirstTags.join(',') + ") AS p JOIN \"http://localhost/mv/class/testphotos1/user\" AS u ON (p.\"http://code.google.com/p/tagont/hasVisibility\" = u.\"http://xmlns.com/foaf/0.1/mbox\");",
        function(__pE, __pR)
        {
          var __lUOINames = new Array(); if (__pR) { __pR.forEach(function(___pEl) { __lUOINames.push(___pEl[0]["http://code.google.com/p/tagont/hasVisibility"]); }); }
          console.log("users that have one of " + _lFirstTags.join(',') + ": " + __lUOINames.join(','));
          _pOnSuccess(__pR);
        });
    }
    var lCountUserPhotos = function(_pUserName, _pOnSuccess) // Count how many photos the specified user can see (asynchronous).
    {
      var _lSS = new InstrSeq();
      var _lTags = {} // Accumulate user and group privileges.
      _lSS.push( // Check user privileges.
        function()
        {
          lMvStore.mvsql(
            "SELECT * FROM \"http://localhost/mv/class/testphotos1/privilege\" WHERE \"http://code.google.com/p/tagont/hasVisibility\"='" + _pUserName + "';",
            function(__pE, __pR)
            {
              var __lExpected_usPriv = lInMemoryChk.getTags_usPriv(_pUserName).sort();
              __pR.forEach(function(___pEl){ _lTags[___pEl["http://code.google.com/p/tagont/hasTagLabel"]] = 1; });
              var __lActual_usPriv = Object.keys(_lTags).slice(0).sort();
              console.log("user " + _pUserName + " has user privilege tags " + __lActual_usPriv.join(','));
              if (JSON.stringify(__lExpected_usPriv) != JSON.stringify(__lActual_usPriv))
                console.log("WARNING: expected user-privilege tags " + __lExpected_usPriv.join(','));
              _lSS.next();
            });
        });
      _lSS.push( // Check group privileges.
        function()
        {
          lMvStore.mvsql(
            "SELECT * FROM \"http://localhost/mv/class/testphotos1/privilege\" AS p JOIN \"http://localhost/mv/class/testphotos1/user\"('" + _pUserName + "') AS u ON (p.\"http://code.google.com/p/tagont/hasVisibility\" = u.\"http://xmlns.com/foaf/0.1/member/adomain:Group\");",
            function(__pE, __pR)
            {
              var __lExpectedTags = lInMemoryChk.getUserTags(_pUserName).sort();
              __pR.forEach(function(___pEl){ _lTags[___pEl[0]["http://code.google.com/p/tagont/hasTagLabel"]] = 1; });
              var __lActualTags = Object.keys(_lTags).slice(0).sort();
              console.log("user " + _pUserName + " has tags " + __lActualTags.join(','));
              if (JSON.stringify(__lExpectedTags) != JSON.stringify(__lActualTags))
                console.log("WARNING: expected tags " + __lExpectedTags.join(','));
              _lSS.next();
            });
        });
      var _lUniquePhotos = {};
      _lSS.push( // Count unique photos.
        function()
        {
          var __lTags = new Array(); Object.keys(_lTags).forEach(function(___pEl) { __lTags.push("'" + ___pEl + "'"); });
          lMvStore.mvsql(
            "SELECT * FROM \"http://localhost/mv/class/testphotos1/photo\" AS p JOIN \"http://localhost/mv/class/testphotos1/tagging\" AS t ON (p.\"http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#hasHash\" = t.\"http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#hasHash\") WHERE t.\"http://code.google.com/p/tagont/hasTagLabel\" IN (" + __lTags.join(',') + ");",
            function(__pE, __pR)
            {
              __pR.forEach(function(___pEl){ _lUniquePhotos[___pEl[0]["http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#hasHash"]] = 1; });
              if (Object.keys(_lUniquePhotos).length != __pR.length)
                console.log("non-unique: " + __pR.length + " unique: " + Object.keys(_lUniquePhotos).length);
              _lSS.next();
            });
        });
      _lSS.push(
        function()
        {
          var __lActualCount = Object.keys(_lUniquePhotos).length;
          lChkCount("photos that can be viewed by " + _pUserName, lInMemoryChk.countPhotos(_pUserName), __lActualCount);
          _pOnSuccess(__lActualCount);
        });
      _lSS.start();
    };
    var lClassesExist = false;
    var lOnSelectClasses = function(pError, pResponse) { console.log("substep " + lSS.curstep()); if (pError) console.log("\n*** ERROR: " + pError + "\n"); else { console.log("Result from step " + lSS.curstep() + ":" + JSON.stringify(pResponse)); lClassesExist = (pResponse && pResponse.length > 0); lSS.next(); } }
    lSS.push(function() { console.log("Creating classes."); lMvStore.mvsql("SELECT * FROM mv:ClassOfClasses WHERE BEGINS(mv:classID, 'http://localhost/mv/class/testphotos1/');", lOnSelectClasses); });
    lSS.push(function() { if (lClassesExist) lSS.next(); else lMvStore.mvsql("CREATE CLASS \"http://localhost/mv/class/testphotos1/photo\" AS SELECT * WHERE \"http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#hasHash\" IN :0 AND EXISTS(\"http://www.w3.org/2001/XMLSchema#date\") AND EXISTS(\"http://www.w3.org/2001/XMLSchema#time\") AND EXISTS(\"http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#fileUrl\") AND EXISTS (\"http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#fileName\");", lSS.simpleOnResponse); });
    lSS.push(function() { if (lClassesExist) lSS.next(); else lMvStore.mvsql("CREATE CLASS \"http://localhost/mv/class/testphotos1/tag\" AS SELECT * WHERE \"http://code.google.com/p/tagont/hasTagLabel\" in :0 AND NOT EXISTS(\"http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#hasHash\") AND NOT EXISTS(\"http://code.google.com/p/tagont/hasVisibility\");", lSS.simpleOnResponse); });
    lSS.push(function() { if (lClassesExist) lSS.next(); else lMvStore.mvsql("CREATE CLASS \"http://localhost/mv/class/testphotos1/tagging\" AS SELECT * WHERE EXISTS(\"http://code.google.com/p/tagont/hasTagLabel\") AND \"http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#hasHash\" in :0;", lSS.simpleOnResponse); });
    lSS.push(function() { if (lClassesExist) lSS.next(); else lMvStore.mvsql("CREATE CLASS \"http://localhost/mv/class/testphotos1/user\" AS SELECT * WHERE \"http://xmlns.com/foaf/0.1/mbox\" in :0 AND EXISTS(\"http://www.w3.org/2002/01/p3prdfv1#user.login.password\") AND EXISTS(\"http://xmlns.com/foaf/0.1/member/adomain:Group\");", lSS.simpleOnResponse); });
    lSS.push(function() { if (lClassesExist) lSS.next(); else lMvStore.mvsql("CREATE CLASS \"http://localhost/mv/class/testphotos1/privilege\" AS SELECT * WHERE \"http://code.google.com/p/tagont/hasTagLabel\" in :0 AND EXISTS(\"http://code.google.com/p/tagont/hasVisibility\");", lSS.simpleOnResponse); });
    lSS.push(function() { console.log("Deleting old data."); lMvStore.mvsql("DELETE FROM \"http://localhost/mv/class/testphotos1/photo\";", lSS.simpleOnResponse); });
    lSS.push(function() { lMvStore.mvsql("DELETE FROM \"http://localhost/mv/class/testphotos1/tag\";", lSS.simpleOnResponse); });
    lSS.push(function() { lMvStore.mvsql("DELETE FROM \"http://localhost/mv/class/testphotos1/tagging\";", lSS.simpleOnResponse); });
    lSS.push(function() { lMvStore.mvsql("DELETE FROM \"http://localhost/mv/class/testphotos1/user\";", lSS.simpleOnResponse); });
    lSS.push(function() { lMvStore.mvsql("DELETE FROM \"http://localhost/mv/class/testphotos1/privilege\";", lSS.simpleOnResponse); });
    var lCntPhotos = 0;
    lSS.push(function() { console.log("Creating a few photos."); lStartTx(lSS); });
    lSS.push(function() { var _lFiles = lWalkDir("../../tests_kernel", ".cpp"); lCntPhotos = _lFiles.length; var _lSS = new InstrSeq(); _lFiles.forEach(function(__pEl) { _lSS.push(function() { lCreatePhoto(__pEl.dirname, __pEl.filename, _lSS.next); }); } ); _lSS.push(lSS.next); _lSS.start(); });
    lSS.push(function() { lCommitTx(lSS); });
    lSS.push(function() { lMvStore.mvsqlCount("SELECT * FROM \"http://localhost/mv/class/testphotos1/photo\";", function(_pE, _pR) { lChkCount("photos", lCntPhotos, _pR); lSS.next(); }); });
    var lSomeTags = ["cousin_vinny", "uncle_buck", "sister_suffragette", "country", "city", "zoo", "mountain_2010", "ocean_2004", "Beijing_1999", "Montreal_2003", "LasVegas_2007", "Fred", "Alice", "sceneries", "artwork"];
    lSS.push(function() { console.log("Creating a few tags."); lStartTx(lSS); });
    lSomeTags.forEach(function(_pTag) { lSS.push(function() { lAssignTagRandomly(_pTag, lSS.next); }); });
    lSS.push(function() { lCommitTx(lSS); });
    var lGroups = ["friends", "family", "public"];
    var lUsers = ["ralph@peanut.com", "stephen@banana.com", "wilhelm@orchestra.com", "sita@marvel.com", "anna@karenina.com", "leo@tolstoy.com", "peter@pan.com", "jack@jill.com", "little@big.com", "john@hurray.com", "claire@obscure.com", "stanley@puck.com", "grey@ball.com", "john@wimbledon.com", "mark@up.com", "sabrina@cool.com"];
    lSS.push(function() { console.log("Creating a few users and groups."); lStartTx(lSS); });
    lUsers.forEach(function(_pUser) { lSS.push(function() { var _lGroup = lGroups[Math.floor(Math.random() * lGroups.length)]; lCreateUser(_pUser, _lGroup, lSS.next); }); });
    lSS.push(function() { lCommitTx(lSS); });
    var lActualUserCount = 0;
    lSS.push(function() { lMvStore.mvsqlCount("SELECT * FROM \"http://localhost/mv/class/testphotos1/user\";", function(_pE, _pR) { lChkCount("users", lUsers.length, _pR); lSS.next(); }); });
    lSS.push(function() { lSelectDistinctGroups(function(_pR) { lChkCount("groups", lGroups.length, _pR.length); lSS.next() }); });
    lSS.push(function() { lAssignGroupPrivilegesRandomly(lSS.next); });
    lSS.push(function() { lAssignUserPrivilegesRandomly(lSS.next); });
    lSS.push(function() { lMvStore.mvsqlCount("SELECT * FROM \"http://localhost/mv/class/testphotos1/privilege\";", function(_pE, _pR) { console.log(_pR + " privileges assigned."); lSS.next(); }); });
    var lTags = null, lUsersOfInterest = null;
    lSS.push(function() { lMvStore.mvsql("SELECT * FROM \"http://localhost/mv/class/testphotos1/tag\";", function(_pE, _pR) { lTags = _pR.slice(0); lSS.next(); }); });
    lSS.push(function() { lGetUsersOfInterest(lTags, function(_pR){ lUsersOfInterest = _pR != null ? _pR.slice(0) : []; lSS.next(); }); });
    lSS.push(function() { var _lSS = new InstrSeq(); lUsersOfInterest.forEach(function(_pEl){ _lSS.push(function(){ lCountUserPhotos(_pEl[0]["http://code.google.com/p/tagont/hasVisibility"], _lSS.next); }); }); _lSS.push(lSS.next); _lSS.start(); });
    lSS.push(function() { console.log("done."); pOnSuccess(); });
    lSS.start();
  },
  
  /**
   * Simple pseudo-application: graph db benchmark.
   *
   * This example uses a mixture of pure mvsql (with json responses) and protobuf.
   * Some of the transaction-control statements are disabled if the connection is not using keep-alive.
   * The data model relies heavily on collections and references.
   *
   * This application reads input files describing a social graph of people with friends,
   * each of which owns a project (or directory) structure, with access privileges granted
   * to their friends on a per-sub-project basis. This project structure also contains photos.
   *
   * The phase1 loads the data into the store.
   * The phase2 performs typical graph queries, using path expressions.
   */

  test_benchgraph1_phase1_load:function(pOnSuccess)
  {
    var lSS = new InstrSeq();
    var lTxSize = 1024;

    // Helper function, to process the text-based input files (people_*.txt, projects_*.txt, photos_*.txt).
    // Can function synchronously or asynchronously, depending on the parameters.
    //   _pFN: the file name
    //   _pDoWhat: a function accepting (a line of text, the line index, and an optional callback to pursue the iteration asynchronously); if this function returns false, the iteration is pursued synchronously by the caller.
    //   _pFinalCallback: an optional callback to be invoked at the end of the chain, in the case of asynchronous execution; accepts the total number of lines.
    //   _pMaxLineLen: an optional specification of the maximum expected length for any given line in the text file.
    var lProcessLines = function(_pFN, _pDoWhat, _pFinalCallback, _pMaxLineLen)
    {
      var _lMaxLineLen = _pMaxLineLen || 256;
      var _lTotalLineCount = 0;
      var _lF = lib_fs.openSync(_pFN, "r");
      lib_assert.ok(undefined != _lF);
      var _lPos = 0;
      var _lNextLine =
        function()
        {
          var __lRead = lib_fs.readSync(_lF, _lMaxLineLen, _lPos, "ascii").toString();
          var __lMatch = (undefined != __lRead) ? __lRead.match(/(.*)\n/) : null;
          if (undefined == __lMatch)
          {
            lib_fs.close(_lF);
            if (undefined != _pFinalCallback)
              _pFinalCallback(_lTotalLineCount);
            return _lTotalLineCount;
          }
          var __lLine = __lMatch[1];
          _lPos += __lLine.length + 1;
          _lTotalLineCount++;
          if (undefined == _pDoWhat || !_pDoWhat(__lLine, _lTotalLineCount, _lNextLine))
            return _lNextLine();
        };
      return _lNextLine();
    }

    // Other helpers.
    var lProcessLinesSync = function(_pFN, _pDoWhat, _pMaxLineLen) { return lProcessLines(_pFN, function(__pLine, __pLineNum) { if (undefined != _pDoWhat) { _pDoWhat(__pLine, __pLineNum); } return false; }); }
    var lCountLinesInFile = function(_pFN) { return lProcessLinesSync(_pFN); }
    var lParallelExecHub = function(_pMaxCount, _pCallback)
    {
      var _lCount = 0;
      return {punch:function() { _lCount++; if (_lCount == _pMaxCount) { _pCallback(); } else { lib_assert.ok(_lCount < _pMaxCount); } }};
    }
    var lStartTx = function(_pSS) { if (lMvStore.keptAlive()) { lMvStore.mvsql("START TRANSACTION;", _pSS.next); } else { _pSS.next(); } }
    var lCommitTx = function(_pSS) { if (lMvStore.keptAlive()) { lMvStore.mvsql("COMMIT;", _pSS.next); } else { _pSS.next(); } }
    var lWritePercent = function(_pPercent) { var _lV = _pPercent.toFixed(0); for (var _i = 0; _i < _lV.length + 1; _i++) { process.stdout.write("\b"); } process.stdout.write("" + _pPercent.toFixed(0) + "%"); }

    // Declaration of classes.
    var lClassesExist = false;
    var lOnSelectClasses = function(pError, pResponse) { console.log("substep " + lSS.curstep()); if (pError) console.log("\n*** ERROR: " + pError + "\n"); else { console.log("Result from step " + lSS.curstep() + ":" + JSON.stringify(pResponse)); lClassesExist = (pResponse && pResponse.length > 0); lSS.next(); } }
    lSS.push(function() { console.log("Creating classes."); lMvStore.mvsql("SELECT * FROM mv:ClassOfClasses WHERE BEGINS(mv:classID, 'http://localhost/mv/class/benchgraph1/');", lOnSelectClasses); });
    lSS.push(function() { if (lClassesExist) lSS.next(); else lMvStore.mvsql("CREATE CLASS \"http://localhost/mv/class/benchgraph1/orgid\" AS SELECT * WHERE \"http://localhost/mv/property/benchgraph1/orgid\" IN :0;", lSS.simpleOnResponse); });
    lSS.push(function() { if (lClassesExist) lSS.next(); else lMvStore.mvsql("CREATE CLASS \"http://localhost/mv/class/benchgraph1/fid\" AS SELECT * WHERE \"http://localhost/mv/property/benchgraph1/fid\" in :0;", lSS.simpleOnResponse); });

    // Definition of input files.
    var lPeopleLineCount = 0, lProjectsLineCount = 0, lPhotosLineCount = 0;
    var lParamsActual = "50_10_50_100";
    var lPeopleFileName = "./tests_data/people_" + lParamsActual + ".txt";
    var lProjectsFileName = "./tests_data/projects_" + lParamsActual + ".txt";
    var lPhotosFileName = "./tests_data/photos_" + lParamsActual + ".txt";
    lSS.push(
      function() 
      {
        lPeopleLineCount = lCountLinesInFile(lPeopleFileName);
        lProjectsLineCount = lCountLinesInFile(lProjectsFileName);
        lPhotosLineCount = lCountLinesInFile(lPhotosFileName);
        lSS.next();
      });

    // First scan of the 'people' file: create all people.
    lSS.push(function() { console.log("Creating " + lPeopleLineCount + " people..."); lStartTx(lSS); });
    lSS.push(
      function()
      {
        // Parallel execution (create all people in parallel)...
        var _lHub = lParallelExecHub(lPeopleLineCount, lSS.next);
        lProcessLinesSync(
          lPeopleFileName,
          function(_pLine, _pLineCount)
          {
            var _lM = _pLine.match(/^\(([0-9]+)\s+\'([A-Za-z\s]+)\'\s+\'([A-Za-z\s]+)\'\s+\'([A-Za-z\s]+)\'\s+\'([A-Za-z\-\s]+)\'\s+\'([A-Za-z\s]+)\'\s+\'([A-Z][0-9][A-Z]\s+[0-9][A-Z][0-9])\'/)
            if (undefined != _lM)
            {
              lMvStore.mvsql(
                "INSERT (\"http://localhost/mv/property/benchgraph1/orgid\", \"http://localhost/mv/property/benchgraph1/firstname\", \"http://localhost/mv/property/benchgraph1/middlename\", \"http://localhost/mv/property/benchgraph1/lastname\", \"http://localhost/mv/property/benchgraph1/occupation\", \"http://localhost/mv/property/benchgraph1/country\", \"http://localhost/mv/property/benchgraph1/postalcode\") VALUES (" +
                _lM[1] + ", '" +
                _lM[2] + "', '" + _lM[3] + "', '" + _lM[4] + "', '" +
                _lM[5] + "', '" + _lM[6] + "', '" + _lM[7] + "');", _lHub.punch());
            }
            else console.log("WARNING: Couldn't match attributes on " + _pLine);
          });
      });
    lSS.push(function() { lCommitTx(lSS); });
    lSS.push(function() { console.log("Created " + lPeopleLineCount + " people."); lSS.next(); });

    // Second scan of the 'people' file: create all relationships.
    var lRelCount = 0;
    lSS.push(function() { console.log("Creating relationships..."); lStartTx(lSS); });
    lSS.push(
      function()
      {
        // Serial execution (create relationships one person at a time)...
        process.stdout.write("  0%");
        lProcessLines(
          lPeopleFileName,
          function(_pLine, _pLineCount, _pNextLine)
          {
            var __lM = _pLine.match(/^\(([0-9]+).*\(([0-9\s]*)\)\)$/)
            if (undefined != __lM)
            {
              // Parallel execution (create all of a person's relationships in parallel)...
              var __lRefs = __lM[2].split(" ");
              if (0 == __lRefs.length) { _pNextLine(); return; }
              var __lPID1;
              var __lSS = new InstrSeq();
              var __lHub = lParallelExecHub(__lRefs.length, __lSS.next);
              __lSS.push(function() { lMvStore.mvsql("SELECT * FROM \"http://localhost/mv/class/benchgraph1/orgid\"(" + __lM[1] + ");", function(__pE, __pR) { __lPID1 = __pR[0].id; __lSS.next() }); });
              __lSS.push(
                function()
                {
                  for (var ___iR = 0; ___iR < __lRefs.length; ___iR++)
                  {
                    var ___lIter =
                      function()
                      {
                        var ___lPID2;
                        var ___lSS = new InstrSeq();
                        ___lSS.push(function() { lMvStore.mvsql("SELECT * FROM \"http://localhost/mv/class/benchgraph1/orgid\"(" + __lRefs[___iR] + ");", function(___pE, ___pR) { ___lPID2 = ___pR[0].id; ___lSS.next() }); });
                        ___lSS.push(function() { lRelCount++; lMvStore.mvsql("UPDATE @" + __lPID1.toString(16) + " ADD \"http://localhost/mv/property/benchgraph1/friendof\"=@" + ___lPID2.toString(16) + ";", __lHub.punch); });
                        ___lSS.start();
                      };
                    ___lIter();
                  }
                });
              /* Note: This could be enabled once task #223 is resolved.
              __lSS.push(
                function()
                {
                  for (var ___iR = 0; ___iR < __lRefs.length; ___iR++)
                  {
                    lMvStore.mvsql("UPDATE @" + __lPID1.toString(16) + " ADD \"http://localhost/mv/property/benchgraph1/friendof\"=(SELECT * FROM \"http://localhost/mv/class/benchgraph1/orgid\"(" + __lRefs[___iR] + "));", __lHub.punch());
                    lRelCount++;
                  }
                });
              */
              __lSS.push(function() { lWritePercent(100.0 * _pLineCount / lPeopleLineCount); __lSS.next(); });
              __lSS.push(_pNextLine);
              __lSS.start();
            }
            else console.log("WARNING: Couldn't match attributes on " + _pLine);
            return true;
          },
          lSS.next);
      });
    lSS.push(function() { lCommitTx(lSS); });
    lSS.push(function() { console.log(" Created " + lRelCount + " relationships."); lSS.next(); });

    // Create the project (aka directory) structure.
    lSS.push(function() { console.log("Creating " + lProjectsLineCount + " projects..."); lStartTx(lSS); });
    lSS.push(
      function()
      {
        // Serial execution (create one project at a time)...
        process.stdout.write("  0%");
        lProcessLines(
          lProjectsFileName,
          function(_pLine, _pLineCount, _pNextLine)
          {
            var _lM = _pLine.match(/^\(([0-9]+)\s+\'([A-Za-z]+)\'\s+([0-9]+)\s+([0-9]+)\)$/)
            if (undefined != _lM)
            {
              var __lSS = new InstrSeq();
              var __lPIDNewProject;
              __lSS.push(
                function()
                {
                  // Create the new project.
                  lMvStore.mvsql(
                    "INSERT (\"http://localhost/mv/property/benchgraph1/fid\", \"http://localhost/mv/property/benchgraph1/fname\", \"http://localhost/mv/property/benchgraph1/access\") VALUES (" +
                    _lM[1] + ", '" +
                    _lM[2] + "', " +
                    _lM[4] + ");", function(__pE, __pR) { __lPIDNewProject = __pR[0].id; __lSS.next() });
                });
              if ("radix" == _lM[2])
              {
                // If the new project is a root project, retrieve its root owner and link to it.
                var __lPIDOwner;
                __lSS.push(function() { lMvStore.mvsql("SELECT * FROM \"http://localhost/mv/class/benchgraph1/orgid\"(" + _lM[4] + ");", function(___pE, ___pR) { __lPIDOwner = ___pR[0].id; __lSS.next() }); });
                __lSS.push(
                  function()
                  {
                    lMvStore.mvsql(
                      "UPDATE @" + __lPIDOwner.toString(16) + " SET \"http://localhost/mv/property/benchgraph1/rootproject\"=@" + __lPIDNewProject.toString(16) + ";", __lSS.next);
                  });
              }
              else
              {
                // If the new project is not a root project, link it to its parent.
                var __lPIDParentProject;
                __lSS.push(function() { lMvStore.mvsql("SELECT * FROM \"http://localhost/mv/class/benchgraph1/fid\"(" + _lM[3] + ");", function(___pE, ___pR) { __lPIDParentProject = ___pR[0].id; __lSS.next() }); });
                __lSS.push(
                  function()
                  {
                    lMvStore.mvsql(
                      "UPDATE @" + __lPIDParentProject.toString(16) + " ADD \"http://localhost/mv/property/benchgraph1/children\"=@" + __lPIDNewProject.toString(16) + ";", __lSS.next);
                  });
              }
              __lSS.push(function() { lWritePercent(100.0 * _pLineCount / lProjectsLineCount); __lSS.next(); });
              __lSS.push(_pNextLine);
              __lSS.start();
            }
            else console.log("WARNING: Couldn't match attributes on " + _pLine);
            return true;
          },
          lSS.next);
      });
    lSS.push(function() { lCommitTx(lSS); });
    lSS.push(function() { console.log(" Created " + lProjectsLineCount + " projects."); lSS.next(); });

    // Insert the photos in the project structure.
    lSS.push(function() { console.log("Creating " + lPhotosLineCount + " photos..."); lStartTx(lSS); });
    lSS.push(
      function()
      {
        // Serial execution (create one photo at a time)...
        process.stdout.write("  0%");
        lProcessLines(
          lPhotosFileName,
          function(_pLine, _pLineCount, _pNextLine)
          {
            var _lM = _pLine.match(/^\(([0-9]+)\s+\'([A-Za-z0-9\s]+)\'\s+([0-9]+)\)$/)
            if (undefined != _lM)
            {
              var __lSS = new InstrSeq();
              var __lPIDNewPhoto, __lPIDParent;
              __lSS.push(
                function()
                {
                  // Create the new photo.
                  lMvStore.mvsql(
                    "INSERT (\"http://localhost/mv/property/benchgraph1/fid\", \"http://localhost/mv/property/benchgraph1/pname\") VALUES (" +
                    _lM[1] + ", '" +
                    _lM[2] + "');", function(__pE, __pR) { __lPIDNewPhoto = __pR[0].id; __lSS.next() });
                });
              __lSS.push(function() { lMvStore.mvsql("SELECT * FROM \"http://localhost/mv/class/benchgraph1/fid\"(" + _lM[3] + ");", function(___pE, ___pR) { __lPIDParent = ___pR[0].id; __lSS.next() }); });
              __lSS.push(
                function()
                {
                  lMvStore.mvsql(
                    "UPDATE @" + __lPIDParent.toString(16) + " ADD \"http://localhost/mv/property/benchgraph1/children\"=@" + __lPIDNewPhoto.toString(16) + ";", __lSS.next);
                });
              __lSS.push(function() { lWritePercent(100.0 * _pLineCount / lPhotosLineCount); __lSS.next(); });
              __lSS.push(_pNextLine);
              __lSS.start();
            }
            else console.log("WARNING: Couldn't match attributes on " + _pLine);
            return true;
          },
          lSS.next);
      });
    lSS.push(function() { lCommitTx(lSS); });
    lSS.push(function() { console.log(" Created " + lPhotosLineCount + " photos."); lSS.next(); });
    lSS.push(function() { console.log("done."); pOnSuccess(); });
    lSS.start();
  },
  test_benchgraph1_phase2_queries:function(pOnSuccess)
  {
    var lSS = new InstrSeq();

    // Use a sample of ~20 people in the middle of the range.
    var lNumPeople = 0;
    var lPeopleSample = new Array();
    lSS.push(
      function()
      {
        lMvStore.mvsqlProto(
          "SELECT * FROM \"http://localhost/mv/class/benchgraph1/orgid\";",
          function(_pE, _pR)
          {
            lNumPeople = _pR.length;
            for (var _iP = 10; _iP < Math.min(_pR.length - 10, 30); _iP++)
              lPeopleSample.push(_pR[_iP]);
            lSS.next();
          })
      });

    // Helper for breadth-first search, with full trace of the solution.
    function BFSearchCtx(pStartOrgid, pEndOrgid, pCallback)
    {
      var _mStartOrgid = pStartOrgid; // The starting value of http://localhost/mv/property/benchgraph1/orgid.
      var _mEndOrgid = pEndOrgid; // The target value of http://localhost/mv/property/benchgraph1/orgid.
      var _mFinalCallback = pCallback; // The final callback to invoke when a solution is found or when no solution can be found.
      var _mBwHops = {}; // A dictionary of backword hops (how did we reach B? from A).
      var _mNextLevel = []; // An array of PINs to visit next time we recurse one level deeper.
      var _mFound = false; // Whether a solution was found.
      var _lGetSolution = function(_pLastOrgid)
      {
        var __lHops = new Array();
        var __iFrom = _pLastOrgid;
        while (__iFrom != _mStartOrgid)
        {
          __lHops.push(__iFrom);
          __iFrom = _mBwHops[__iFrom];
        }
        return __lHops;
      }
      var _lRecurseOne = function(_pSS, _pPIN, _pLevel)
      {
        _pSS.push(
          function()
          {
            if (_mFound) { return; }
            lMvStore.mvsqlProto(
              "SELECT * FROM @" + _pPIN.pid.toString(16) + ".\"http://localhost/mv/property/benchgraph1/friendof\";",
              function(__pE, __pR)
              {
                var __lFromOrgid = _pPIN.get("http://localhost/mv/property/benchgraph1/orgid");
                for (var __iP = 0; __iP < __pR.length; __iP++)
                {
                  var __lP = __pR[__iP];
                  var __lToOrgid = __lP.get("http://localhost/mv/property/benchgraph1/orgid");
                  if (__lToOrgid == _mEndOrgid)
                  {
                    _mFound = true;
                    _mFinalCallback(_lGetSolution(__lFromOrgid));
                    break;
                  }
                  if (__lToOrgid in _mBwHops) { continue; }
                  _mBwHops[__lToOrgid] = __lFromOrgid;
                  _mNextLevel.push(__lP);
                }
                _pSS.next();
              });
          });
      }
      var _lRecurse = function(_pSS, _pPINs, _pLevel)
      {
        /*
        var __lTrace = "";
        for (var __j = 0; __j < _pPINs.length; __j++)
          __lTrace += _pPINs[__j].get("http://localhost/mv/property/benchgraph1/orgid") + " ";        
        console.log("level " + _pLevel + ": " + __lTrace);
        */

        // Collect all friends of the next level.
        _mNextLevel = [];
        for (var __i = 0; __i < _pPINs.length; __i++)
          _lRecurseOne(_pSS, _pPINs[__i], _pLevel);
        // Process them.
        _pSS.push(
          function()
          {
            if (_mFound || 0 == _mNextLevel.length) { _pSS.next(); return; }
            var ___lSS2 = new InstrSeq();
            _lRecurse(___lSS2, _mNextLevel.slice(0), _pLevel + 1);
            ___lSS2.push(_pSS.next);
            ___lSS2.start();
          });
      }
      this.solve = function()
      {
        var __lSS = new InstrSeq();
        var __lStartPIN;
        __lSS.push(
          function()
          {
            lMvStore.mvsqlProto(
              "SELECT * FROM \"http://localhost/mv/class/benchgraph1/orgid\"(" + _mStartOrgid + ");",
              function(_pE, _pR)
              {
                assertValidResult(_pR);
                __lStartPIN = _pR[0];
                __lSS.next();
              });
          });
        __lSS.push(
          function()
          {
            var ___lSS2 = new InstrSeq();
            _lRecurse(___lSS2, [__lStartPIN], 1);
            ___lSS2.push(__lSS.next);
            ___lSS2.start();
          });
        __lSS.push(function() { if (!_mFound) { _mFinalCallback(null); } });
        __lSS.start();
      }
    }

    // Queries - case 1: can person1 reach person2?
    lSS.push(
      function()
      {
        console.log("case 1:");
        var _lSS = new InstrSeq();
        var _lQ =
          function(_pP)
          {
            _lSS.push(
              function()
              {
                var __lP2 = lNumPeople - _pP;
                new BFSearchCtx(
                  _pP, __lP2,
                  function(__pSolution)
                  {
                    if (undefined == __pSolution) { console.log("" + _pP + " cannot reach " + __lP2); }
                    else { console.log("" + _pP + " can reach " + __lP2 + " via " + __pSolution.join(" via ")); }
                    _lSS.next(); 
                  }).solve();
              });
          }
        for (var _iP = 10; _iP < lNumPeople; _iP += 10) { _lQ(_iP); }
        _lSS.push(lSS.next);
        _lSS.start();
      });

    // Queries - case 2: find the set of all people that have friends among A's friends
    lSS.push(
      function()
      {
        console.log("case 2:");
        var _lSS = new InstrSeq();
        var _lQ =
          function(_pP)
          {
            _lSS.push(
              function()
              {
                lMvStore.mvsql(
                  "SELECT * FROM @" + _pP.pid.toString(16) + ".\"http://localhost/mv/property/benchgraph1/friendof\".\"http://localhost/mv/property/benchgraph1/friendof\"[@ <> @" + _pP.pid.toString(16) + "];",
                  function(__pE, __pR)
                  {
                    console.log("" + __pR.length + " people have friends in common with " + _pP.get("http://localhost/mv/property/benchgraph1/firstname") + " " + _pP.get("http://localhost/mv/property/benchgraph1/middlename") + " " + _pP.get("http://localhost/mv/property/benchgraph1/lastname"));
                    _lSS.next();
                  });
              });
          }
        for (var _iP = 0; _iP < lPeopleSample.length; _iP++) { _lQ(lPeopleSample[_iP]); }
        _lSS.push(lSS.next);
        _lSS.start();
      });

    // Queries - case 3: find all photos owned by A, i.e. present in the project structure owned by A
    lSS.push(
      function()
      {
        console.log("case 3:");
        var _lSS = new InstrSeq();
        var _lQ =
          function(_pP)
          {
            _lSS.push(
              function()
              {
                lMvStore.mvsql(
                  "SELECT * FROM @" + _pP.pid.toString(16) + ".\"http://localhost/mv/property/benchgraph1/rootproject\".\"http://localhost/mv/property/benchgraph1/children\"{*}[exists(\"http://localhost/mv/property/benchgraph1/pname\")];",
                  function(__pE, __pR)
                  {
                    console.log(_pP.get("http://localhost/mv/property/benchgraph1/firstname") + " " + _pP.get("http://localhost/mv/property/benchgraph1/middlename") + " " + _pP.get("http://localhost/mv/property/benchgraph1/lastname") + " has projects containing " + __pR.length + " photos");
                    _lSS.next();
                  });
              });
          }
        for (var _iP = 0; _iP < lPeopleSample.length; _iP++) { _lQ(lPeopleSample[_iP]); }
        _lSS.push(lSS.next);
        _lSS.start();
      });

    // Queries - case 4: find all photos given access to A by his friends
    lSS.push(
      function()
      {
        console.log("case 4:");
        var _lSS = new InstrSeq();
        var _lQ =
          function(_pP)
          {
            _lSS.push(
              function()
              {
                lMvStore.mvsql(
                  "SELECT * FROM @" + _pP.pid.toString(16) + ".\"http://localhost/mv/property/benchgraph1/friendof\".\"http://localhost/mv/property/benchgraph1/rootproject\".\"http://localhost/mv/property/benchgraph1/children\"{*}[\"http://localhost/mv/property/benchgraph1/access\"=" + _pP.get("http://localhost/mv/property/benchgraph1/orgid") + "].\"http://localhost/mv/property/benchgraph1/children\"[exists(\"http://localhost/mv/property/benchgraph1/pname\")];",
                  function(__pE, __pR)
                  {
                    console.log(_pP.get("http://localhost/mv/property/benchgraph1/firstname") + " " + _pP.get("http://localhost/mv/property/benchgraph1/middlename") + " " + _pP.get("http://localhost/mv/property/benchgraph1/lastname") + " has access to " + __pR.length + " photos shared by friends");
                    _lSS.next();
                  });
              });
          }
        for (var _iP = 0; _iP < lPeopleSample.length; _iP++) { _lQ(lPeopleSample[_iP]); }
        _lSS.push(lSS.next);
        _lSS.start();
      });
                
    lSS.push(function() { console.log("done."); pOnSuccess(); });
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
    if (_lTN.slice(0, 4) != "test") { console.log("\nSkipping " + _lTN); iTest++; lRunNextTest(pOnlyThisTest); return; }
    if (undefined != pOnlyThisTest && pOnlyThisTest != _lTN) { iTest++; lRunNextTest(pOnlyThisTest); return; }
    console.log("\nRunning " + _lTN + ":");
    console.time(_lTN);
    lTests[_lTN](function(){ console.timeEnd(_lTN); iTest++; lRunNextTest(pOnlyThisTest); });
  };
lRunNextTest(/*"test_types"*/);
