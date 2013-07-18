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

// This module performs a simple store dump, and demonstrates basic querying options.

// Resolve dependencies.
var lib_affinity = require('./lib/affinity-client');
var lib_assert = require('assert');

// Connect to the Affinity server.
var lAffinity = lib_affinity.createConnection("http://nodetests:@localhost:4560/db/", {keepalive:true});
var InstrSeq = lib_affinity.instrSeq;

// Define the parameters of the query.
var lQuery = "SELECT *";
var lPageSize = 200;
var lProtoOut = true;
var lQueryPage =
  function(_pSS, _pOffset)
  {
    _pSS.push(
      function()
      {
        lAffinity.q(
          lQuery,
          function(__pE, __pR) { lib_assert.ok(undefined == __pE && undefined != __pR); console.log(__pR); _pSS.next(); },
          {limit:lPageSize, offset:_pOffset});
      });
  }

// Go.
var lTotalCount = 0;
var lSS = new InstrSeq();
lSS.push(function() { lAffinity.qCount(lQuery, function(_pE, _pR) { lib_assert.ok(undefined == _pE && undefined != _pR); console.log("TOTAL COUNT: " + _pR); lTotalCount = _pR; lSS.next(); }); });
lSS.push(function() { setTimeout(lSS.next, 1000); });
lSS.push(
  function()
  {
    var _lSS = new InstrSeq();
    for (var _i = 0; _i < lTotalCount; _i += lPageSize)
      lQueryPage(_lSS, _i);
    _lSS.push(lSS.next);
    _lSS.start();
  });
lSS.push(function() { console.log("Done."); lAffinity.terminate(); });
lSS.start();
