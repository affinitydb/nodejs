/**************************************************************************************

Copyright © 2004-2011 VMware, Inc. All rights reserved.

**************************************************************************************/

// This file executes the pathSQL test suite (aka 'test4pathsql' aka *.sql).

// Resolve dependencies.
var lib_affinity = require('./lib/affinity-client');
var lib_assert = require('assert');
var lib_fs = require('fs');
var lib_sys = require('sys');

// Connect to the Affinity server.
var lAffinity = lib_affinity.createConnection("http://test4pathsql:@localhost:4560/db/", {keepalive:true});
var InstrSeq = lib_affinity.instrSeq;
var DELAY_BETWEEN_OPS_IN_MS = 200;
var STOP_AT_FIRST_ERROR = false;

// Define a helper to walk a directory structure.
// Returns an array of all files with extension pExt under pDir (recursively; synchronous[!]).
function walkDir(pDir, pExt)
{
  var lResult = new Array();
  var lRegexp = new RegExp(pExt + "$");
  var lProcessFN = function(_pFN) { var _lFFN = pDir + "/" + _pFN; if (lib_fs.statSync(_lFFN).isFile()) { if (-1 != _lFFN.search(lRegexp)) { lResult.push({dirname:pDir, filename:_pFN}); } } else lResult = lResult.concat(walkDir(_lFFN, pExt)); }
  var lFN = lib_fs.readdirSync(pDir);
  lFN.forEach(lProcessFN);  
  return lResult;
}

// Processing of one .sql file.
function processSqlFile(pFileName, pFinalCallback, pMaxLineLen)
{
  console.log("processing file " + pFileName);
  var lMaxLineLen = pMaxLineLen || 1024;
  var lTotalLineCount = 0;
  var lF = lib_fs.openSync(pFileName, "r");
  lib_assert.ok(undefined != lF);
  var lPos = 0;
  var lCurrentLine = "";
  var lProcessLine =
    function(_pLine, _pContinue)
    {
      lCurrentLine = lCurrentLine + _pLine.replace(/\s*/, "");
      var _lEos = lCurrentLine.indexOf(";"); // REVIEW: maybe too simplistic.
      if (-1 == _lEos)
        { _pContinue(); return; }
      var _lFullStmt = lCurrentLine;
      lCurrentLine = "";
      console.log("running: " + _lFullStmt);
      if (undefined != _lFullStmt.match(/drop/i)) // Special request.
      {
        var lAfyS2 = lib_affinity.createConnection("http://test4pathsql:@localhost:4560/drop/", {keepalive:false});
        lAfyS2.rawGet(function(_pE, _pR) { console.log("obtained: " + (undefined == _pE ? "ok" : _pE)); setTimeout(_pContinue, DELAY_BETWEEN_OPS_IN_MS);});
      }
      else if (undefined != _lFullStmt.match(/open/i)) // Nothing to do (store will auto-open on demand).
        { console.log("obtained: ok"); setTimeout(_pContinue, DELAY_BETWEEN_OPS_IN_MS); }
      else if (undefined != _lFullStmt.match(/execute/i) || // Ignored (not supported here).
        undefined != _lFullStmt.match(/display/i) ||
        undefined != _lFullStmt.match(/explain/i) ||
        undefined != _lFullStmt.match(/prepare/i))
          { console.log("ignored"); _pContinue(); }
      else // Standard request.
      {
        lAffinity.qProto(
          _lFullStmt,
          function(_pE, _pR)
          {
            console.log("obtained: " + ((undefined != _pE) ? "failure" : JSON.stringify(_pR)));
            if (undefined != _pE && STOP_AT_FIRST_ERROR) { throw _pE; }
            setTimeout(_pContinue, DELAY_BETWEEN_OPS_IN_MS);
          });
      }
    }
  var lProcessNextLine =
    function()
    {
      var _lRead = lib_fs.readSync(lF, lMaxLineLen, lPos, "utf8").toString();
      var _lEol = _lRead.indexOf("\n");
      if (-1 == _lEol)
      {
        lib_fs.close(lF);
        if (undefined != pFinalCallback)
          pFinalCallback(lTotalLineCount);
        return lTotalLineCount;
      }
      var _lLine = _lRead.substr(0, _lEol-1) + " ";
      lPos += _lLine.length + 1;
      var _lComment = _lLine.indexOf("--"); // REVIEW: maybe too simplistic.
      _lLine = (-1 == _lComment) ? _lLine : _lLine.substr(0, _lComment-1);
      // console.log("line: " + _lLine);
      lTotalLineCount++;
      lProcessLine(_lLine, lProcessNextLine);
    };
  return lProcessNextLine();
}

// Processing of all .sql files.
var lSS = new InstrSeq();
var lSqlFiles = walkDir("../../tests_kernel/mvsql", ".sql");
//lSS.push(function() { var _lSS = new InstrSeq(); lSqlFiles.forEach(function(_pEl) { _lSS.push(function() { processSqlFile(_pEl.dirname + "/" + _pEl.filename, _lSS.next); }); }); _lSS.push(lSS.next); _lSS.start(); });
lSS.push(function() { var _lF = lSqlFiles[4]; processSqlFile(_lF.dirname + "/" + _lF.filename, lSS.next); });
lSS.push(function() { console.log("Done."); lAffinity.terminate(); });
lSS.start();
