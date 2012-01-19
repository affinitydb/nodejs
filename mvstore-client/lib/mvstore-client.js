/**************************************************************************************

Copyright © 2004-2011 VMware, Inc. All rights reserved.

**************************************************************************************/

var lib_fs = require('fs');
var lib_http = require('http');
var lib_https = require('https');
var lib_url = require('url');
var lib_events = require('events');
var lib_crypto = require('crypto');
var lib_protobuf = require('protobuf_for_node');
var lib_assert = require('assert');

// Define a small helper to serialize steps.
// Note: This is similar in purpose to nodejs's 'step' module.
module.exports.instrSeq = function()
{
  var iSubStep = 0;
  var lSubSteps = new Array();
  var lThis = this;
  this.next = function() { iSubStep++; if (iSubStep < lSubSteps.length) lSubSteps[iSubStep](); }
  this.push = function(pSubStep) { lSubSteps.push(pSubStep); }
  this.start = function() { iSubStep = 0; lSubSteps[iSubStep](); }
  this.curstep = function() { return iSubStep; }
  this.simpleOnResponse = function(pError, pResponse)
  {
    if (pError) { console.error("\n*** ERROR in substep " + lThis.curstep() + ": " + pError + "\n"); }
    else { console.log("Result from substep " + lThis.curstep() + ":" + JSON.stringify(pResponse)); lThis.next(); }
  }
}

// Connection to a mvStore server.
// Notes:
//   . The connection is 'single-threaded' in the sense that it can only handle
//     one transaction (i.e. one long http request) at a time; 'multi-threading'
//     implies multiple connection objects.
//   . The connection object is effectively a meta-connection: it generates
//     a series of http connections, in a single logical thread.
//   . In a sense the connection object hides the notion of network connection,
//     in favor of the notion of transaction: the public interface
//     only needs to expose startTx and commitTx (even though underneath
//     a transaction implies a longer-lasting http connection).
//   . if pOptions specifies {keepalive:true}, then the connection becomes
//     a real, physical, long-lasting connection, that holds resources
//     on the server as long as it exists.
module.exports.createConnection = function createConnection(pUrl, pOptions)
{
  // HTTP client config.
  var lUri = lib_url.parse(pUrl);
  lUri.httpProtocol = (lUri.protocol == 'https:') ? lib_https : lib_http;
  lUri.__proto__ = { host:'127.0.0.1:4560', port:4560, hostname:'127.0.0.1', pathname:"/db/" }; // Note: Assignment to __proto__ means that any member of lUri not explicitly defined will default to the members of this __proto__.

  // Protobuf config.
  var lMvProtoSchema = new lib_protobuf.Schema(lib_fs.readFileSync('mvstore.desc'));
  var lMvStream = lMvProtoSchema['MVStorePB.MVStream'];
  var lTxCtx = null; // Represents the current txctx/long-http-request for this connection.
  var lNextCid = 1; // Next stmt id.
  var lKeepAlive = {on:(("keepalive" in pOptions) && pOptions.keepalive), client:null}

  // Protobuf constants.
  // Note: The unofficial trick of using enums as constants is simply not available in this protobuf implementation.
  var MV_FIRST_PROPID = 255/*SP_MAX*/ + 1;
  var MV_TIME_OFFSET = (new Date(1970,1,1).valueOf() - new Date(1601,1,1).valueOf()) * 1000;
  var EID_COLLECTION = 4294967295;
  var EID_LAST_ELEMENT = 4294967294;
  var EID_FIRST_ELEMENT = 4294967293;
  var RT_PIDS = 2;
  var RT_PINS = 0;
  
  // Other constants.
  var PREFIX_JS_PROP = "http://localhost/mv/property/1.0/";
  var PROP_JS_PROTOTYPE = PREFIX_JS_PROP + "js_prototype"; // Reference to the object's prototype pin (Review: Use Mark's special prop).
  var PROP_JS_PROTOTYPE_HASH = PROP_JS_PROTOTYPE + "/hash"; // Prototype hash, used to identify distinct prototypes (in a global, deterministic way).
  var PROP_JS_CONSTRUCTOR = PREFIX_JS_PROP + "js_constructor"; // Reference to the object's constructor pin (including 'static' methods).
  var PROP_JS_CONSTRUCTOR_HASH = PROP_JS_CONSTRUCTOR + "/hash"; // Constructor hash, used to identify distinct constructors (in a global, deterministic way).
  var PROP_JS_KIND = PREFIX_JS_PROP + "js_kind"; // Kind of the special js entity (constructor, prototype).
  var PROP_JS_CONSTRUCTOR_NAME = PREFIX_JS_PROP + "js_constructor_name"; // Name of the constructor.
  var PREFIX_JS_METHOD = PREFIX_JS_PROP + "js_method/"; // Prototype method prefix (for any property of a prototype defining a method).
  var PREFIX_JS_STATICMETHOD = PREFIX_JS_PROP + "js_staticmethod/"; // Constructor-attached method prefix (for any property of the constructor defining a static method).
  var PREFIX_JS_MEMBER = PREFIX_JS_PROP + "js_member/"; // Object member prefix (for any property defining an object's data member).
  var PREFIX_JS_PARAMETERS = PREFIX_JS_PROP + "js_parameters/"; // Prefix for any property defining a function's parameters (collection of parameter names).

  /**
   * Private implementation helpers.
   */

  // Generic http GET/POST to the mvstore server (supports both the pure pathSQL path and the protobuf path).
  function private_http(_pPath, _pBody, _pCallback)
  {
    var _lStream = _pCallback ? null : new lib_events.EventEmitter();
    var _lIsGoodStatus = function(__pResponse) { return __pResponse.statusCode >= 200 && __pResponse.statusCode <= 204; }
    var _lOnError = function(__pErr){ console.error("ERROR (private_http._lOnError): " + __pErr); if (_pCallback) _pCallback(__pErr); else _lStream.emit('error', __pErr); }
    var _lJsonOut = _pPath.search(/o=json/) >= 0;
    var _lOnResponse =
      _lJsonOut ?
        function(__pResponse)
        {
          var __lResponse = "";
          __pResponse.setEncoding('utf8');
          __pResponse.on('data', function(___pChunk) { if (_pCallback) { __lResponse += ___pChunk; } else _lStream.emit('data', ___pChunk); });
          __pResponse.on('end',
            function()
            {
              if (_pCallback)
              {
                if (_lIsGoodStatus(__pResponse))
                  { _pCallback(null, (undefined != __lResponse && __lResponse.length > 0) ? JSON.parse(__lResponse.replace(/\s+/g, " ")) : null); }
                else
                {
                  console.error("ERROR " + __pResponse.statusCode + " (private_http): " + __lResponse + "(on " + decodeURIComponent(_pPath) + ")");
                  _pCallback(__lResponse, null);
                }
              }
              else
                { _lStream.emit('end'); }
            });
        }
        :
        function(__pResponse)
        {
          var __lCursor = 0;
          var __lResponse = null;
          __pResponse.setEncoding('binary');
          __pResponse.on('data',
            function(___pChunk)
            {
              if (_pCallback)
              {
                var ___lChunkLen = Buffer.byteLength(___pChunk, "binary");
                var ___lTempBuffer = new Buffer(__lCursor + ___lChunkLen);
                if (__lResponse != undefined && __lResponse.length > 0)
                  { __lResponse.copy(___lTempBuffer, 0, 0, __lResponse.length); }
                ___lTempBuffer.write(___pChunk, __lCursor, "binary");
                __lResponse = ___lTempBuffer;
                __lCursor += ___lChunkLen;
              }
              else
                { _lStream.emit('data', ___pChunk); } 
            });
          __pResponse.on('end',
            function()
            {
              if (_pCallback)
              {
                if (_lIsGoodStatus(__pResponse))
                  { _pCallback(null, __lResponse ? __lResponse.slice(0, __lCursor) : null); }
                else
                {
                  console.error("ERROR (private_http): " + __lResponse + " (on " + decodeURIComponent(_pPath) + ")");
                  _pCallback(__lResponse ? __lResponse.slice(0, __lCursor) : "error", null);
                }
              }
              else
                { _lStream.emit('end'); }
            });
          __pResponse.on('continue', function() { lib_assert.fail("private_http: received a 'continue' event (not yet handled)."); });
        }
    var _lHeaders = [["Host", lUri.hostname]];
    if (_pBody)
    {
      _lHeaders.push(["Content-Length", _pBody.length]);
      _lHeaders.push(["Content-Type", "application/octet-stream"]);
    }
    if (lUri.auth)
    {
      _lHeaders.push(["Authorization", "Basic " + new Buffer(lUri.auth, "ascii").toString("base64")]);
    }
    if (lKeepAlive.on)
    {
      _lHeaders.push(["Connection", "Keep-Alive"]);
      if (undefined == lKeepAlive.client)
        lKeepAlive.client = lUri.httpProtocol.createClient(lUri.port, lUri.hostname);
      var _lRequest = lKeepAlive.client.request(((_pBody != undefined) ? "POST" : "GET"), _pPath, _lHeaders);
      lib_assert.ok(_lRequest.shouldKeepAlive);
      _lRequest.on('response', _lOnResponse);
      _lRequest.on('error', _lOnError);      
      if (_pBody) _lRequest.write(_pBody);
      _lRequest.end();
    }
    else
    {
      var _lRequest = lUri.httpProtocol.request({host:lUri.hostname, method:((_pBody != undefined) ? "POST" : "GET"), path:_pPath, port:lUri.port, headers:_lHeaders}, _lOnResponse);
      lib_assert.ok(!_lRequest.shouldKeepAlive);
      _lRequest.on('error', _lOnError);
      if (_pBody) _lRequest.write(_pBody);
      _lRequest.end();
    }
    return _lStream;
  }
  
  // Long http POST, for transactional protobuf (used by TxCtx only).
  // [Ideally, would only useful/necessary when pOptions does not specify keepalive. See the note below in the constructor.]
  function private_longhttp(pPath, pCallback)
  {
    //
    // Note:
    //   Ideally, for simplicity and consistency, I would not use private_longhttp at all, when running in keep-alive mode.
    //   However, currently mvstore treats each protobuf message as a topmost transaction, and aborts it
    //   if it doesn't commit by itself. For that reason, I'm still forced to go via private_longhttp
    //   in keep-alive mode. That implies that plain (non-protobuf, non-pathsqlProto) pathSQL requests can't be used
    //   inside protobuf transactions.
    //
    // if (lKeepAlive.on) throw "private_longhttp is not expected to be used in a keep-alive connection";
    //
    this.mPath = pPath;
    this.mCallback = pCallback;
    // Note: Strange logic in nodejs's http.js (the ClientRequest constructor), where if headers is an array it's handled differently than if not (e.g. chunkedEncoding will be set right away vs not).
    this.mHeaders = [["Host", lUri.hostname], ["Content-Type", "application/octet-stream"]];
    if (lKeepAlive.on) { this.mHeaders.push(["Connection", "Keep-Alive"]); }
    if (lUri.auth) { this.mHeaders.push(["Authorization", "Basic " + new Buffer(lUri.auth, "ascii").toString("base64")]); }
    this.mRequest = null;
    this.mResponse = null;
    this.mEnded = false;
  }
  private_longhttp.prototype.beginlongpost = function()
  {
    var _lOnError = function(__pErr){ console.error("ERROR (private_longhttp._lOnError): " + __pErr); this.mCallback(__pErr); }
    var _lThis = this;
    var _lOnResponse =
      function(__pResponse)
      {
        __pResponse.setEncoding('binary');
        __pResponse.on('data',
          function(___pChunk)
          {
            var ___lChunkLen = Buffer.byteLength(___pChunk, "binary");
            // console.log("private_longhttp._lOnResponse: received " + ___lChunkLen + " bytes");
            _lThis.mResponse = new Buffer(___lChunkLen);
            _lThis.mResponse.write(___pChunk, 0, "binary");
            // dbg_savePBStr(_lThis.mResponse);
            // console.log("private_longhttp._lOnResponse: received " + JSON.stringify(lMvStream.parse(_lThis.mResponse)));
            if (_lThis.mEnded)
            {
              // console.log("private_longhttp._lOnResponse: ended the request");
              _lThis.mRequest.destroy();
              _lThis.mRequest = null;
              _lThis.mEnded = false;
              lTxCtx = null;
            }
            _lThis.mCallback(null, (undefined != _lThis.mResponse) ? _lThis.mResponse : ""); _lThis.mResponse = null;
          });
        __pResponse.on('end', function() { lib_assert.fail("private_longhttp: received an 'end' event (not yet handled)"); });
        __pResponse.on('close', function() { /*lib_assert.fail("private_longhttp: received a 'close' event (not yet handled)");*/ });
        __pResponse.on('continue', function() { lib_assert.fail("private_longhttp: received a 'continue' event (not yet handled)"); });
      };
    /*
    // Note:
    //   This way of sharing the same client object between private_http and private_longhttp
    //   doesn't work well, as a means of combining protobuf and non-protobuf access paths,
    //   because here in private_longhttp we may hold on to a request for a long time (e.g. higher-level
    //   transactions), and thus keep the client connection busy... if a private_http call happens in that context,
    //   it's doomed to freeze. I see only 3 ways to allow these mixtures:
    //     1. implement everything exclusively via the protobuf path
    //     2. have Mark open-up the transaction tracking related with protobuf
    //        (track the transaction at the level of the session, not the
    //        incoming protobuf message)
    //     3. the current cheat: effectively use 2 connections in parallel
    //        (one for protobuf, and another one for non-protobuf; this means that
    //        semantically the transactions in pure pathSQL happen on a different plane
    //        than the transactions in protobuf)
    if (lKeepAlive.on)
    {
      if (undefined == lKeepAlive.client)
        lKeepAlive.client = lUri.httpProtocol.createClient(lUri.port, lUri.hostname);
      this.mRequest = lKeepAlive.client.request("POST", this.mPath, this.mHeaders);
      lib_assert.ok(this.mRequest.shouldKeepAlive);
      this.mRequest.on('response', _lOnResponse);
      this.mRequest.on('error', _lOnError);      
      this.mEnded = false;
    }
    else
    */
    {
      this.mRequest = lUri.httpProtocol.request({host:lUri.hostname, method:"POST", path:this.mPath, port:lUri.port, headers:this.mHeaders}, _lOnResponse);
      // lib_assert.ok(!this.mRequest.shouldKeepAlive);
      this.mRequest.on('error', _lOnError);
      this.mEnded = false;
    }
  }
  private_longhttp.prototype.continuelongpost = function(pMsg)
  {
    // console.log("private_longhttp.continuelongpost: sent " + pMsg.length + " bytes");
    this.mResponse = null;
    //if (!this.mRequest._headerSent) { this.mRequest._send(this.mRequest._header, "ascii"); this.mRequest._headerSent = true; }
    this.mRequest._send(pMsg, "binary");
  }
  private_longhttp.prototype.endlongpost = function(pTxopAlone)
  {
    // console.log("private_longhttp.endlongpost");
    this.mEnded = true;
    this.mRequest.end();

    if (pTxopAlone)
    {
      // Note:
      //   This is more or less a hack... TX_COMMIT+flush alone don't produce a response at the moment...
      //   Mark says this is unfinished functionality. In python, a similar hack was performed, but
      //   it was more subtle and I had actually not noticed it.
      // console.log("private_longhttp.endlongpost: ended the request");
      this.mRequest.abort();
      this.mRequest = null;
      this.mEnded = false;
      lTxCtx = null;
      this.mCallback(null, null);
    }
  }

  // Helper for options supported by the mvserver on the url.
  function appendUrlOptions(_pUrl, _pOptions)
  {
    var _lUrl = _pUrl;
    if (undefined != _pOptions)
    {
      if ("offset" in _pOptions) _lUrl += "&offset=" + _pOptions.offset;
      if ("limit" in _pOptions) _lUrl += "&limit=" + _pOptions.limit;
    }
    return _lUrl;
  }

  // Clone helper.
  function deepCloneObject(_pO)
  {
    if (_pO == null || typeof _pO == "number") return _pO;
    switch (_pO.constructor)
    {
      default: lib_assert.fail("deepCloneObject: unexpected type " + _pO.constructor); break;
      case Number: case Boolean: case String: case Ref: // These are immutable or copied by value anyway.
        return _pO;
      case Date: case RegExp: case Function: 
        return new _pO.constructor(_pO);
      case Object: case Array:
      {
        var _lR = new _pO.constructor();
        for (var _iP in _pO)
          _lR[_iP] = deepCloneObject(_pO[_iP]);
        return _lR;
      }
    }
    return _pO;
  }

  // Debugging helpers.
  function dbg_stackTrace()
  {
    console.log(new Error().stack);
  }
  function dbg_savePBStr(_pMsgStr) // Similar to mvstore.py.
  {
    _lF = lib_fs.openSync("/tmp/myproto.bin", "w+");
    lib_fs.writeSync(_lF, _pMsgStr, 0, _pMsgStr.length, 0);
    lib_fs.close(_lF);
  }

  // Protobuf: transaction context (similar to mvstore.py).
  function private_TxCtx()
  {
    this.mLabels = new Array(); // Optional labels for this transaction stack (to help debugging etc.); also keeps track of nested transactions.
    this.mSegments = new Array(); // Accumulated serialized segments.
    this.mUpdates = new Array(); // Accumulated pin updates (not yet serialized into a next segment).
    this.mUpdatesSent = new Array();
    this.mPropDict = new private_PropDict(); // Global property dictionary for the whole txctx.
    this.mCallbacks = []; // Stack of user-specified callbacks (for http completion).
    var _lThis = this;
    var _lCallback = function(_pE, _pR) { _lThis._doCallback(_pE, _pR); }
    // if (!lKeepAlive.on) // See the note in private_longhttp...
    {
      this.mLongHttp = new private_longhttp(lUri.pathname + "?i=proto&o=proto", _lCallback);
      this.mLongHttp.beginlongpost();
    }
    this.mMustFlushOp = false; // The current op will require a flush + pushData.
  }
  private_TxCtx.inExplicitTx = function()
  {
    return (undefined != lTxCtx);
  }
  private_TxCtx.isTxCbObj = function(pCallbackObj)
  {
    return (undefined != pCallbackObj) && (typeof(pCallbackObj) == "object") && ("txend" in pCallbackObj) && (typeof(pCallbackObj.txend) == "function");
  }
  private_TxCtx.beginOp = function(pResultHandler, pOptions)
  {
    // Defines a scope for any operation performed during a transaction.
    // Returns a function that the (internal) caller must invoke to complete the operation.
    // That returned function will ultimately invoke pResultHandler
    // (synchronously or asynchronously, depending on circumstances).
    // Notes:
    //   . Here we are at the crossroads of a number of notions:
    //     1. the atomic operation itself (one step of a transaction),
    //     2. the notion of implicit transaction,
    //     3. the fact that operations may require an immediate
    //        roundtrip to mvstore, to obtain store-generated ids,
    //     4. the response parsing bound to an operation, if any, and
    //     5. the need to effect this parsing immediately,
    //        in the context of a longer transaction (the fate of future
    //        operations in that same transaction may be dependent on
    //        the output of earlier operations).
    //   . pOptions.mustFlush is just a means of centralizing and controlling
    //     the use of this protobuf feature, in a way consistent with
    //     the ability to parse the results (with pResultHandler).
    //   . pResultHandler is responsible to handle the results returned by mvstore
    //     for this operation (i.e. parse, extract new ids etc.).
    //   . When pOptions.mustFlush is true, a pResultHandler must be specified (even for operations that
    //     generate no interesting response), because this handler also serves
    //     the purpose of asynchronous continuation; otherwise, the only case where pResultHandler
    //     is optional is within the context of an explicit transaction.
    //   . Asynchronous continuation may happen as the result of two situations:
    //     1. running the operation as an implicit transaction, and
    //     2. running an operation that requires immediate results (with pOptions.mustFlush).
    //   . Every flush implies a call to _pushData.
    //   . In the future, we might find ways of clustering together a series of operations
    //     that do require an output but that don't require it immediately (for performance);
    //     however, this seems impossible to predict from within this library, and would
    //     imply further complexification of the interface.
    var _lExplicitTx = private_TxCtx.inExplicitTx();
    if (undefined == pResultHandler && ((undefined != pOptions && pOptions.mustFlush) || !_lExplicitTx))
      { lib_assert.fail("private_TxCtx.beginOp: invalid pResultHandler"); return null; }
    if (!_lExplicitTx)
      { lTxCtx = new private_TxCtx(); }
    lTxCtx._pushTxLabel(null, _lExplicitTx);
    lTxCtx.mMustFlushOp = _lExplicitTx ? (undefined == pOptions ? false : pOptions.mustFlush) : true;
    return function() { lTxCtx._endOp(null, pResultHandler); }
  }
  private_TxCtx.makeHandler_passthrough = function(pCallbackObj)
  {
    if (pCallbackObj)
    {
      if (!private_TxCtx.isTxCbObj(pCallbackObj))
        { lib_assert.fail("private_TxCtx.makeHandler_passthrough: invalid completion callback"); return null; }
      return pCallbackObj.txend;
    }
    return function(_pE, _pR) {};
  }
  private_TxCtx.makeHandler_onReceivePINs = function(pCallbackObj)
  {
    if (pCallbackObj)
    {
      if (!private_TxCtx.isTxCbObj(pCallbackObj))
        { lib_assert.fail("private_TxCtx.makeHandler_onReceivePINs: invalid completion callback"); return null; }
      var _lH =
        function(_pE, _pR)
        {
          if (_pE) { console.error("ERROR (private_TxCtx.makeHandler_onReceivePINs._lH): " + _pE); pCallbackObj.txend(_pE, null); return; }
          pCallbackObj.txend(null, private_receivePB._createPINs(_pR, new private_PropDict()));
        }
      return _lH;
    }
    return function(_pE, _pR) {};
  }
  private_TxCtx.makeHandler_onNewEids = function(pCallbackObj)
  {
    if (!private_TxCtx.isTxCbObj(pCallbackObj))
      { lib_assert.fail("private_TxCtx.makeHandler_onNewEids: invalid completion callback"); return null; }
    var _lH = 
      function(_pE, _pR, _pTxCtx)
      {
        if (_pE) { console.error("ERROR (private_TxCtx.makeHandler_onNewEids._lH): " + _pE); pCallbackObj.txend(_pE, null); return; }
        var __lParser = new private_PBResponseParser(_pR);
        var __lPINUpdates = _pTxCtx._getRelevantUpdates();
        pCallbackObj.txend(null, __lParser.finalizeUpdates(__lPINUpdates));
      };
    return _lH;
  }
  private_TxCtx.makeHandler_onNewPIDs = function(_pPINDescriptions, pCallbackObj)
  {
    if (!private_TxCtx.isTxCbObj(pCallbackObj))
      { lib_assert.fail("private_TxCtx.makeHandler_onNewPIDs: invalid completion callback"); return null; }
    var _lH =
      function(_pE, _pR)
      {
        if (_pE) { console.error("ERROR (private_TxCtx.makeHandler_onNewPIDs._lH): " + _pE); pCallbackObj.txend(_pE, null); return; }
        var __lParser = new private_PBResponseParser(_pR);
        pCallbackObj.txend(null, __lParser.finalizeCreates(_pPINDescriptions));
      };
    return _lH;
  }
  private_TxCtx.prototype.recordPINUpdate = function(pPINUpdate)
  {
    this.mUpdates.push(pPINUpdate);
  }
  private_TxCtx.prototype.recordAny = function(pStreamObj)
  {
    // Note: pStreamObj can be a single object or an array.

    var _lMustFlushOp = this.mMustFlushOp;
    this.mMustFlushOp = false;
    this._serializePendingUpdates();
    this.mMustFlushOp = _lMustFlushOp;

    if (pStreamObj instanceof Array)
    {
      for (var _iO = 0; _iO < pStreamObj.length; _iO++)
      {
        var _lMsgSer = lMvStream.serialize(pStreamObj[_iO]);
        this.mSegments.push(_lMsgSer);
      }
    }
    else
    {
      var _lMsgSer = lMvStream.serialize(pStreamObj);
      this.mSegments.push(_lMsgSer);
    }
    
    if (this.mMustFlushOp)
      { this.mSegments.push(lMvStream.serialize({flush:[0]})); }
  }
  private_TxCtx.prototype.startTx = function(pTxLabel/*optional*/)
  {
    // Push a label for this transaction (for debugging and to track nested transactions).
    this._pushTxLabel(pTxLabel);
    // Serialize a TX_START protobuf segment.
    var _lMsgSer = lMvStream.serialize({txop:['TX_START']});
    this.mSegments.push(_lMsgSer);
  }
  private_TxCtx.prototype.commitTx = function(pCallback)
  {
    this._endOp('TX_COMMIT', pCallback);
  }
  private_TxCtx.prototype.rollbackTx = function(pCallback)
  {
    this._endOp('TX_ROLLBACK', pCallback);
  }
  private_TxCtx.prototype._serializePendingUpdates = function() // Note: This is called 'capture' in the python library (it also overlaps _applyPINUpdates).
  {
    if (0 == this.mUpdates.length)
      return;
    // console.log("private_TxCtx._serializePendingUpdates: serializing " + this.mUpdates.length + " updates");
    var _lMsgSer = private_sendPB.serialize(private_sendPB.formatPB(this.mUpdates, this.mPropDict), this.mPropDict, this.mMustFlushOp);
    this.mSegments.push(_lMsgSer);
    this.mUpdatesSent = this.mUpdatesSent.concat(this.mUpdates);
    this.mUpdates = new Array();
  }
  private_TxCtx.prototype._pushData = function()
  {
    // console.log("private_TxCtx._pushData: sending " + this.mSegments.length + " segments");
    if (0 == this.mSegments.length)
      { this._doCallback(null, null); return; }

    // Note:
    //   If none of the segments contains a flush request, _pushData can freeze, as there would be no response from the server.
    //   We could monitor this situation, or even force a flush here. From a debugging point of view, the flush is immediately
    //   visible with the trace produced a few lines below.
    // this.mSegments.push(lMvStream.serialize({flush:[0]}));

    // Concatenate all segments into one final buffer.
    var _iS, _lLen = 0
    for (_iS = 0; _iS < this.mSegments.length; _iS++)
      _lLen += this.mSegments[_iS].length;
    var _lSer = new Buffer(_lLen);
    var _lPos = 0;
    for (_iS = 0; _iS < this.mSegments.length; _lPos += this.mSegments[_iS].length, _iS++)
    {
      // console.log("private_TxCtx._pushData: sending " + JSON.stringify(lMvStream.parse(this.mSegments[_iS])));
      this.mSegments[_iS].copy(_lSer, _lPos);
    }

    // Clear all segments.
    this.mSegments = new Array();

    // Send the buffer.
    this._pushData_impl(_lSer);
  }
  private_TxCtx.prototype._pushData_impl = function(_pMsgSer)
  {
    if (undefined == this.mLongHttp)
    {
      var _lThis = this;
      private_http(lUri.pathname + "?i=proto&o=proto", _pMsgSer, function(__pE, __pR) { _lThis._doCallback(__pE, __pR); });
      return;
    }
    this.mLongHttp.continuelongpost(_pMsgSer);
  }
  private_TxCtx.prototype._pushTxLabel = function(pTxLabel, pExplicit)
  {
    this.mLabels.push((undefined == pTxLabel) ? ("no-label [" + (pExplicit ? "explicit" : "implicit") + " tx]") : pTxLabel);
    // console.log("private_TxCtx._pushTxLabel: " + this.mLabels[this.mLabels.length - 1]);
  }
  private_TxCtx.prototype._endOp = function(pTxop, pCallback)
  {
    // console.log("private_TxCtx._endOp: " + this.mLabels[this.mLabels.length - 1] + " with " + pTxop + " (" + (this.mLabels.length - 1) + ")");

    // Remember the callback for this transaction/operation (i.e. this flush).
    this.mCallbacks.push(pCallback);
    // console.log("private_TxCtx._endOp: pushed callback (" + this.mCallbacks.length + "): " + pCallback);
    
    // Serialize any pending PIN update.
    this._serializePendingUpdates();

    // Serialize pTxop, if any (commit/rollback).
    if (undefined != pTxop)
    {
      var _lMsgSer = lMvStream.serialize({txop:[pTxop], flush:[0]});
      this.mMustFlushOp = true;
      this.mSegments.push(_lMsgSer);
    }

    // console.log("private_TxCtx._endOp: " + this.mSegments.length + " segments");

    // Transfer the 'must flush' state to a local variable, and clear.
    var _lMustFlushOp = this.mMustFlushOp;
    this.mMustFlushOp = false;

    // Pop the label attached to this transaction/operation.
    this.mLabels.pop();
    
    // Handle completion of a topmost transaction.
    // This includes clearing 'this' from lTxCtx.
    if (0 == this.mLabels.length)
    {
      var _lNumSegments = this.mSegments.length;
      this._pushData();
      if (undefined == this.mLongHttp)
        { lTxCtx = null; }
      else // Note: Missing this step would cause hang at exit due to pending callbacks.
        { this.mLongHttp.endlongpost(undefined != pTxop && 1 == _lNumSegments); }
    }

    // Otherwise, handle completion of an operation that must flush.
    else if (_lMustFlushOp)
      { this._pushData(); }

    // Otherwise, default to just invoking the callback, if one was specified.
    else if (undefined == pTxop && pCallback && this.mCallbacks.length > 0)
      { this._doCallback(null, null); }
  }
  private_TxCtx.prototype._doCallback = function(pE, pR)
  {
    if (this.mCallbacks.length > 0)
    {
      var _lCallback = this.mCallbacks.pop(); // Note: When we invoke the callback it may itself queue up more callbacks, so it's important to pop it right away.
      // console.log("private_TxCtx._doCallback: received e=" + JSON.stringify(pE) + " r=" + JSON.stringify(pR ? lMvStream.parse(pR) : null));
      if (pE) { console.error("ERROR (private_TxCtx._doCallback): " + pE); _lCallback(pE, null, this); }
      else { _lCallback(pE, pR, this); }
      return;
    } 
    lib_assert.fail("private_TxCtx._doCallback: uninitialized user callback; received e=" + JSON.stringify(pE) + " r=" + JSON.stringify(pR ? lMvStream.parse(pR) : null));
  }
  private_TxCtx.prototype._getRelevantUpdates = function()
  {
    var _lPINUpdates = new Array();
    for (var _iP = 0; _iP < this.mUpdatesSent.length; _iP++)
      if (this.mUpdatesSent[_iP].mExpectNewIDs)
        _lPINUpdates.push(this.mUpdatesSent[_iP]);
    return _lPINUpdates;
  }

  // Protobuf: Collection accessor object, mimicking the Array interface, and representing collections conveniently.
  // Note: Similar to mvstore.py's PIN.Collection, except that here it's a pure interface, instantiated in a transient fashion (upon PIN.get('prop'))...
  // Note: _pPINAccessor is a function returning a {mPID:..., mPropVals:..., mExtras:...} object pointing to a in-memory PIN's internal details.
  function Collection(_pPINAccessor, _pPropName)
  {
    var _mPINAccessor = _pPINAccessor;
    var _mPropName = _pPropName;
    var _mThis = this;
    var _mUpdateLength = function() { _mThis.length = _mPINAccessor().mPropVals.length; return _mThis.length; }
    this.collprop = _mPropName;
    this.length = _mUpdateLength();
    this.push = function(pNewValue, pCallbackObj/*optional*/)
    {
      // Setup async handling of the pushData to mvstore (required to obtain resulting eid).
      // Note:
      //   Logically, we should always flush here, since a new eid will be produced.
      //   But for flexibility, we interpret the absence of pCallbackObj as a willingness to forfeit the eid update.
      //   This option is only available in an explicit transaction, because otherwise asynchronous continuation is required anyway...
      var _lMustFlush = private_TxCtx.isTxCbObj(pCallbackObj);
      if (!_lMustFlush && !private_TxCtx.inExplicitTx()) { lib_assert.fail("Collection.push: invalid pCallbackObj"); return _mThis.length; }
      var _lResultHandler = _lMustFlush ? private_TxCtx.makeHandler_onNewEids(pCallbackObj) : private_TxCtx.makeHandler_passthrough(null);
      var _lOpEnd = private_TxCtx.beginOp(_lResultHandler, {mustFlush:_lMustFlush});
      if (undefined == _lOpEnd) { return _mThis.length; }
      // First, record a pin update in the current txctx.
      var _lUpdPV = {}, _lExtras = {};
      _lUpdPV[_mPropName] = pNewValue;
      _lExtras[_mPropName] = {eid:EID_LAST_ELEMENT/*Review:arguable*/, op:'OP_ADD'};
      lTxCtx.recordPINUpdate(new private_PINUpdate(_mPINAccessor, _lUpdPV, _lExtras, true));
      // Second, perform the standard modif on the in-memory array.
      var _lRet = _mPINAccessor().mPropVals[_mPropName].push(pNewValue);
      _mPINAccessor().mExtras[_mPropName].push(_lExtras[_mPropName]);
      _mUpdateLength();
      // Third, complete the operation + invoke pCallbackObj.txend, and return the expected result.
      _lOpEnd();
      return _lRet;
    }
    this.pop = function(pCallbackObj/*optional*/)
    {
      // Setup async handling of the pushData to mvstore (required in the case of an implicit transaction).
      if (!private_TxCtx.isTxCbObj(pCallbackObj) && !private_TxCtx.inExplicitTx()) { lib_assert.fail("Collection.pop: invalid pCallbackObj"); return null; }
      var _lOpEnd = private_TxCtx.beginOp(private_TxCtx.makeHandler_passthrough(pCallbackObj), {mustFlush:false});
      if (undefined == _lOpEnd) { return null; }
      // First, record a pin update in the current txctx.
      var _lPropExtras = _mPINAccessor().mExtras[_mPropName];
      var _lUpdPV = {}, _lExtras = {};
      _lUpdPV[_mPropName] = 0;
      _lExtras[_mPropName] = {eid:_lPropExtras[_lPropExtras.length - 1].eid, op:'OP_DELETE'};
      lTxCtx.recordPINUpdate(new private_PINUpdate(_mPINAccessor, _lUpdPV, _lExtras, false));
      // Second, perform the standard modif on the in-memory array.
      var _lRet = _mPINAccessor().mPropVals[_mPropName].pop();
      _lPropExtras.pop();
      _mUpdateLength();      
      // Third, complete the operation + invoke pCallbackObj.txend (if necessary: implicit tx), and return the expected result.
      _lOpEnd();
      return _lRet;
    }
    this.shift = function(pCallbackObj/*optional*/)
    {
      // Setup async handling of the pushData to mvstore (required in the case of an implicit transaction).
      var _lDefaultRet = _mPINAccessor().mPropVals[_mPropName].length > 0 ? _mPINAccessor().mPropVals[_mPropName].length[0] : null;
      if (!private_TxCtx.isTxCbObj(pCallbackObj) && !private_TxCtx.inExplicitTx()) { lib_assert.fail("Collection.shift: invalid pCallbackObj"); return _lDefaultRet; }
      var _lOpEnd = private_TxCtx.beginOp(private_TxCtx.makeHandler_passthrough(pCallbackObj), {mustFlush:false});
      if (undefined == _lOpEnd) { return _lDefaultRet; }
      // First, record a pin update in the current txctx.
      var _lPropExtras = _mPINAccessor().mExtras[_mPropName];
      var _lUpdPV = {}, _lExtras = {};
      _lUpdPV[_mPropName] = 0;
      _lExtras[_mPropName] = {eid:_lPropExtras[0].eid, op:'OP_DELETE'};
      lTxCtx.recordPINUpdate(new private_PINUpdate(_mPINAccessor, _lUpdPV, _lExtras, false));
      // Second, perform the standard modif on the in-memory array.
      var _lRet = _mPINAccessor().mPropVals[_mPropName].shift();
      _lPropExtras.shift();
      _mUpdateLength();
      // Third, complete the transaction + invoke pCallbackObj.txend (if necessary: implicit tx), and return the expected result.
      _lOpEnd();
      return _lRet;
    }
    this.unshift = function(/*misc. optional arguments*/)
    {
      // If a pCallbackObj was specified, it's expected to be the last argument.
      var _lCallbackObj = null;
      var _lArgLen = arguments.length;
      var _lLastArg = arguments[_lArgLen - 1];
      if (private_TxCtx.isTxCbObj(_lLastArg)) { _lCallbackObj = _lLastArg; _lArgLen -= 1; }
      // Setup async handling of the pushData to mvstore (required to obtain new resulting eids).
      // Note: See the note in 'Collection.push'.
      var _lMustFlush = (undefined != _lCallbackObj);
      if (!_lMustFlush && !private_TxCtx.inExplicitTx()) { lib_assert.fail("Collection.unshift: invalid pCallbackObj"); return _lThis.length; }
      var _lResultHandler = _lMustFlush ? private_TxCtx.makeHandler_onNewEids(_lCallbackObj) : private_TxCtx.makeHandler_passthrough(null);
      var _lOpEnd = private_TxCtx.beginOp(_lResultHandler, {mustFlush:_lMustFlush});
      if (undefined == _lOpEnd) { return _lThis.length; }
      for (var _lPos = _lArgLen - 1; _lPos >= 0; _lPos--)
      {
         // First, record a pin update in the current txctx.
         var _lUpdPV = {}, _lExtras = {};
         _lUpdPV[_mPropName] = arguments[_lPos];
         _lExtras[_mPropName] = {eid:EID_FIRST_ELEMENT, op:'OP_ADD_BEFORE'};
         lTxCtx.recordPINUpdate(new private_PINUpdate(_mPINAccessor, _lUpdPV, _lExtras, true));
         // Second, perform the standard modif on the in-memory array.
         _mPINAccessor().mPropVals[_mPropName].unshift(arguments[_lPos]);
         _mPINAccessor().mExtras[_mPropName].unshift(_lExtras[_mPropName]);
      }
      // Third, complete the operation + invoke pCallbackObj.txend, and return the expected result.
      _lOpEnd();
      return _mUpdateLength();
    }
    this.sort = function(/*optional pCallbackObj and pOrderFunc*/)
    {
      // REVIEW:
      //   Instead of re-implementing sort here, with a weak algorithm,
      //   we could instead let the standard array do its sorting,
      //   and just patch the eid order (like we do in python).
      
      // Get the optional pCallbackObj and order func.
      var _lCallbackObj = null;
      var _lOrderFunc = function(_p1, _p2) { return _p1 < _p2 ? -1 : (_p1 == _p2 ? 0 : 1); }
      for (var _iArg = 0; _iArg < arguments.length; _iArg++)
      {
        if (private_TxCtx.isTxCbObj(arguments[_iArg])) _lCallbackObj = arguments[_iArg];
        else if (typeof(arguments[_iArg]) == "function") _lOrderFunc = arguments[_iArg];
      }
      if ((undefined == _lCallbackObj) && !private_TxCtx.inExplicitTx()) { lib_assert.fail("Collection.sort: invalid pCallbackObj"); return _mThis; }
      var _lOpEnd = private_TxCtx.beginOp(private_TxCtx.makeHandler_passthrough(_lCallbackObj), {mustFlush:false});
      if (undefined == _lOpEnd) { return _mThis; }
      
      // Proceed with bubble sort.
      var _lPropVals = _mPINAccessor().mPropVals[_mPropName];
      var _lPropExtras = _mPINAccessor().mExtras[_mPropName];
      var _lSwap = function(_pArray, _pPos1, _pPos2) { var __lTmp = _pArray[_pPos1]; _pArray[_pPos1] = _pArray[_pPos2]; _pArray[_pPos2] = __lTmp; }
      var _lSwapped;
      do
      {
        _lSwapped = false;
        for (var _lPos = 0; _lPos < _lPropVals.length - 1; _lPos++)
        {
          var _lUpdPV = {}, _lExtras = {};
          if (_lOrderFunc(_lPropVals[_lPos], _lPropVals[_lPos + 1]) > 0)
          {
              // First, record a pin update in the current txctx.
              _lUpdPV[_mPropName] = _lPropExtras[_lPos].eid;
              _lExtras[_mPropName] = {eid:_lPropExtras[_lPos + 1].eid, op:'OP_MOVE_BEFORE', type:'VT_UINT'};
              lTxCtx.recordPINUpdate(new private_PINUpdate(_mPINAccessor, _lUpdPV, _lExtras, false));
              // Second, perform the standard modif on the in-memory array.
              _lSwap(_lPropVals, _lPos, _lPos + 1);
              _lSwap(_lPropExtras, _lPos, _lPos + 1);
              _lSwapped = true;
          }
        }
      } while (_lSwapped);
      // Third, complete the operation + invoke pCallbackObj.txend (if necessary: implicit tx), and return the expected result.
      _lOpEnd();
      return _mThis;
    }
    this.reverse = function(pCallbackObj/*optional*/)
    {
      if (!private_TxCtx.isTxCbObj(pCallbackObj) && !private_TxCtx.inExplicitTx()) { lib_assert.fail("Collection.reverse: invalid pCallbackObj"); return _mThis; }
      var _lOpEnd = private_TxCtx.beginOp(private_TxCtx.makeHandler_passthrough(pCallbackObj), {mustFlush:false});
      if (undefined == _lOpEnd) { return _mThis; }
      var _lPropVals = _mPINAccessor().mPropVals[_mPropName];
      var _lPropExtras = _mPINAccessor().mExtras[_mPropName];
      if (_lPropVals.length <= 1) return _mThis;
      // First, record a series of pin updates in the current txctx.
      var _lPivot = _lPropExtras[_lPropVals.length - 1].eid;
      for (var _lPos = _lPropVals.length - 2; _lPos >= 0; _lPos--)
      {
        var _lUpdPV = {}, _lExtras = {};
        _lUpdPV[_mPropName] = _lPivot;
        _lExtras[_mPropName] = {eid:_lPropExtras[_lPos].eid, op:'OP_MOVE', type:'VT_UINT'};
        _lPivot = _lPropExtras[_lPos].eid;
        lTxCtx.recordPINUpdate(new private_PINUpdate(_mPINAccessor, _lUpdPV, _lExtras, false));
      }
      // Second, perform the standard modif on the in-memory array.
      _lPropVals.reverse();
      _lPropExtras.reverse();
      // Third, complete the transaction + invoke pCallbackObj.txend (if necessary: implicit tx), and return the expected result.
      _lOpEnd();
      return _mThis;
    }    
    this.splice = function(pStart, pDeleteCount/*optional pValues and pCallbackObj*/)
    {
      // If a pCallbackObj was specified, it's expected to be the last argument.
      var _lCallbackObj = null;
      var _lArgLen = arguments.length;
      var _lLastArg = arguments[_lArgLen - 1];
      if (private_TxCtx.isTxCbObj(_lLastArg)) { _lCallbackObj = _lLastArg; _lArgLen -= 1; }
      // Setup async handling of the pushData to mvstore (required to obtain new resulting eids).
      // Note: See the note in 'Collection.push'.
      // Note: We could be more selective here (in cases where no new eid is actually expected).
      var _lMustFlush = (undefined != _lCallbackObj);
      if (!_lMustFlush && !private_TxCtx.inExplicitTx()) { lib_assert.fail("Collection.splice: invalid pCallbackObj"); return []; }
      var _lResultHandler = _lMustFlush ? private_TxCtx.makeHandler_onNewEids(_lCallbackObj) : private_TxCtx.makeHandler_passthrough(null);
      var _lOpEnd = private_TxCtx.beginOp(_lResultHandler, {mustFlush:_lMustFlush});
      if (undefined == _lOpEnd) { return []; }
      var _lPropVals = _mPINAccessor().mPropVals[_mPropName];
      var _lPropExtras = _mPINAccessor().mExtras[_mPropName];
      var _lRet;
      // 1. Delete the specified number of elements.
      {
        for (var _lPos = Math.min(_lPropVals.length - 1, pStart + pDeleteCount - 1); _lPos >= pStart; _lPos--)
        {
          // First, record a pin update in the current txctx.
          var _lUpdPV = {}, _lExtras = {};
          _lUpdPV[_mPropName] = 0;
          _lExtras[_mPropName] = {eid:_lPropExtras[_lPos].eid, op:'OP_DELETE'};
          lTxCtx.recordPINUpdate(new private_PINUpdate(_mPINAccessor, _lUpdPV, _lExtras, false));
        }
        // Second, perform the standard modif on the in-memory array.
        _lRet = _lPropVals.splice(pStart, pDeleteCount);
        _lPropExtras.splice(pStart, pDeleteCount);
      }
      // 2. Insert the specified new elements.
      var _lInsertionPivot = (pStart < _lPropVals.length) ? _lPropExtras[pStart].eid : EID_LAST_ELEMENT;
      var _lInsertionOp = (pStart < _lPropVals.length) ? 'OP_ADD_BEFORE' : 'OP_ADD';
      {
        for (var _lArg = 2; _lArg < _lArgLen; _lArg++)
        {
          // First, record a pin update in the current txctx.
          var _lUpdPV = {}, _lExtras = {};
          _lUpdPV[_mPropName] = arguments[_lArg];
          _lExtras[_mPropName] = {eid:_lInsertionPivot, op:_lInsertionOp};
          lTxCtx.recordPINUpdate(new private_PINUpdate(_mPINAccessor, _lUpdPV, _lExtras, true));
          // Second, perform the standard modif on the in-memory array.
          _lPropVals.push(arguments[_lArg]);
          _lPropExtras.push(_lExtras[_mPropName]);
        }
      }
      _mUpdateLength();
      // Third, complete the transaction + invoke pCallbackObj.txend, and return the expected result.
      _lOpEnd();
      return _lRet;
    }
    this.slice = function(pStart, pEnd) { return _mPINAccessor().mPropVals.slice(pStart, pEnd); }
    this.toLocaleString = function() { return _mPINAccessor().mPropVals.toLocaleString(); }
    this.toString = function() { return _mPINAccessor().mPropVals.toString(); }
    this.join = function() { return _mPINAccessor().mPropVals.join(arguments); }
    this.concat = function() { return _mPINAccessor().mPropVals.concat(arguments); }
  }

  // Protobuf: PIN representation.
  // Provides 'set' and 'get' methods to the client (plus the Collection overrides), to enforce
  // the recording of PIN updates upon modification. Stores data privately, in an intermediate form
  // (somewhere between the natural java representation and mvstore.proto's form). This is required
  // in order to give normal access to named properties; the mvstore.proto format also contains many
  // fields that are circumstantial and that don't deserve being cached (e.g. property, op, nValues, rtt, ...).
  // The internal format is very close to mvstore.py's.
  // Note: The constructor expects an array of 3 elements: [PID, propvals, extras].
  function PIN(_pArgs)
  {
    var _mPID = _pArgs[0];
    var _mPropVals = _pArgs[1];
    var _mExtras = _pArgs[2];
    var _mThis = this;
    var _lAccessor = function() { return {mPID:_mPID, mPropVals:_mPropVals, mExtras:_mExtras, mPIN:_mThis}; }
    this.pid = _mPID; // Mostly just for debugging (to avoid completely empty representation).
    this.refresh = function(pCallback) { return private_receivePB.refreshPIN(_lAccessor, pCallback); }
    this.set = function(pPropName, pPropValue, pCallbackObj)
    {
      // Setup async handling of the pushData to mvstore (required to obtain new resulting eids).
      // Note: See the note in 'Collection.push'.
      var _lMustFlush = (pPropValue instanceof Array) || private_TxCtx.isTxCbObj(pCallbackObj);
      if (!_lMustFlush && !private_TxCtx.inExplicitTx()) { lib_assert.fail("PIN.set: invalid pCallbackObj"); return; }
      var _lResultHandler = _lMustFlush ? private_TxCtx.makeHandler_onNewEids(pCallbackObj) : private_TxCtx.makeHandler_passthrough(null);
      var _lOpEnd = private_TxCtx.beginOp(_lResultHandler, {mustFlush:_lMustFlush});
      if (undefined == _lOpEnd) return;

      // Update the in-memory object.
      _mPropVals[pPropName] = pPropValue;
      if (pPropValue instanceof Array) // Replacing the existing value (collection or scalar) with a new collection.
      {
        _mExtras[pPropName] = [];
        for (var _iPV = 0; _iPV < pPropValue.length; _iPV++)
          _mExtras[pPropName].push({eid:EID_LAST_ELEMENT, op:'OP_ADD'});
      }
      else if (_mExtras[pPropName] instanceof Array) // Replacing a collection with a scalar.
        _mExtras[pPropName] = {}; // Note: Type and eid will be overwritten anyway, in this case.

      // Prepare a persistent update.
      var _lUpdPV = {}, _lExtras = {};
      _lUpdPV[pPropName] = pPropValue;
      _lExtras[pPropName] = _mExtras[pPropName];
      lTxCtx.recordPINUpdate(new private_PINUpdate(_lAccessor, _lUpdPV, _lExtras, (pPropValue instanceof Array)));
      
      // Complete the operation + invoke pCallbackObj.txend (if necessary: implicit tx).
      _lOpEnd();
    }
    this.get = function(pPropName) { return (_mPropVals[pPropName] instanceof Array) ? new Collection(_lAccessor, pPropName) : _mPropVals[pPropName]; }
    // ---
    this.toPropValDict = function() { return deepCloneObject(_mPropVals); } // Note: We clone to prevent creating bad habits; this is meant to be used for debugging/introspection.
    this.getExtras = function() { return deepCloneObject(_mExtras); } // Note: This is meant ot be used for debugging/introspection.
  }

  // Protobuf: for VT_URL.
  function Url(_pString)
  {
    var _mString = _pString;
    this.isUrl = function() { return true; }
    this.toString = function() { return _mString; }
  }

  // Protobuf: for VT_REF*.
  function Ref(_pLocalPID, _pIdent, _pProperty, _pEid)
  {
    var _mLocalPID = _pLocalPID;
    var _mIdent = _pIdent;
    var _mProperty = _pProperty;
    var _mEid = _pEid;
    this.isRef = function() { return true; }
    this.get = function() { return {id:_mLocalPID, ident:_mIdent, property:_mProperty, eid:_mEid}; }
    this.getVT = function() { return (undefined != _mEid) ? 'VT_REFIDELT' : ((undefined != _mProperty) ? 'VT_REFIDPROP' : 'VT_REFID'); }
    this.toString = function() { return "@" + _mLocalPID.toString(16) + ((undefined != _mIdent && 0 != _mIdent) ? ("[!" + _mIdent + "]") : "") + ((undefined != _mProperty) ? (".\"" + _mProperty + "\"" + ((undefined != _mEid) ? ("[" + _mEid + "]") : "")) : ""); }
    this.toJSON = this.get;
  }

  // Protobuf: PIN-update representation.
  // Note:
  //   Internal only (never handed out to the client); follows the PIN convention (propvals, extras), but also keeps
  //   a privileged back-pointer accessor to the modified in-memory PIN, to be able to update it upon mvstore completion (e.g. eids etc.).
  function private_PINUpdate(_pPINAccessor, _pUpdatesPropVal, _pUpdatesExtras, _pExpectNewIDs)
  {
    this.mPINAccessor = _pPINAccessor;
    this.mUpdatesPropVal = _pUpdatesPropVal;
    this.mUpdatesExtras = _pUpdatesExtras;
    this.mExpectNewIDs = _pExpectNewIDs;
  }
  private_PINUpdate.prototype.getPID = function() { return this.mPINAccessor().mPID; }
  private_PINUpdate.prototype.getPIN = function() { return this.mPINAccessor().mPIN; }

  // Protobuf: segment, i.e. js instance of a MVStorePB.MVStream segment (to be combined with PINUpdate-s).
  function private_PBSegment() {}

  // Protobuf: dictionary of {propname:propid}.
  function private_PropDict()
  {
    this.mNextID = MV_FIRST_PROPID;
    this.mID2Name = {}
    this.mName2ID = {}
  }
  private_PropDict.prototype.newPIN = function(pPIN) { for (var iV in pPIN) { this.newPropName(iV); } }
  private_PropDict.prototype.newPropName = function(pName) { if (pName in this.mName2ID) return this.mName2ID[pName]; var lNewID = this.mNextID++; this.mName2ID[pName] = lNewID; this.mID2Name[lNewID] = pName; return lNewID; }
  private_PropDict.prototype.getID = function(pName) { return this.mName2ID[pName]; }
  private_PropDict.prototype.getPBRepr = function() { var lPBRepr = new Array(); for (iV in this.mName2ID) lPBRepr.push({str:iV, id:this.mName2ID[iV]}); return lPBRepr; }
  private_PropDict.prototype.registerProps = function(pPB) { if (undefined == pPB) return; for (var iP = 0; iP < pPB.length; iP++) this.registerProp(pPB[iP]); }
  private_PropDict.prototype.registerProp = function(pPBProp) { this.mID2Name[pPBProp.id] = pPBProp.str; this.mName2ID[pPBProp.str] = pPBProp.id; }
  private_PropDict.prototype.getPropName = function(pID) { return this.mID2Name[pID]; }

  // Protobuf save: convert either an array of descriptions of new PINs (js object literals), or an array of
  // private_PINUpdate-s, into their PB representation, serialize the result into a buffer ready to
  // be sent to mvstore, send, and collect the resulting pids/eids, if any.
  function private_sendPB(_pPINs, _pCallback, _pDescr/*optional*/)
  {
    var _lPD = new private_PropDict();
    var _lMsgSer = private_sendPB.serialize(private_sendPB.formatPB(_pPINs, _lPD), _lPD, true);
    return private_sendPB.finalize(_pPINs, _lMsgSer, _pCallback, _pDescr);
  }
  // Static helper to finalize the push to the server.
  private_sendPB.finalize = function(pPINs, pMsgSer, pCallback, pDescr)
  {
    var _lOnPBResult = function(__pE, __pR)
    {
      if (__pE) { console.error("ERROR (private_sendPB.finalize._lOnPBResult): " + pDescr + " failed: " + __pE); pCallback(__pE, null); }
      var __lParser = new private_PBResponseParser(__pR);
      pCallback(null, (pPINs[0] instanceof private_PINUpdate) ? __lParser.finalizeUpdates(pPINs) : __lParser.finalizeCreates(pPINs));
    }
    return private_http(lUri.pathname + "?i=proto&o=proto", pMsgSer, _lOnPBResult);
  }
  // Static helper to serialize the descriptions/updates to a buffer in PB format, ready to be sent to mvstore.
  private_sendPB.formatPB = function(pPINs, pPropDict)
  {
    if (!(pPINs instanceof Array)) throw "private_sendPB.formatPB: pPINs must be an Array.";
    // Note:
    //   pPINs is an array of PIN descriptions (js object literals) or of private_PINUpdate objects, looking like this:
    //
    //     [{"toto":"whatever","tata":123,"titi":"2011-08-02T14:17:50.237Z","tutu":[1,2,3,4]}]
    //
    //       ... or, if it's an update, it could look like this  ...
    //
    //     [
    //       {"mUpdatesPropVal":{"tete":202020},"mUpdatesExtras":{}},
    //       {"mUpdatesPropVal":{"tonton":303030},"mUpdatesExtras":{}},
    //       {"mUpdatesPropVal":{"tutu":5},"mUpdatesExtras":{"tutu":{"eid":4294967294,"op":"OP_ADD"}}}
    //     ]
    // console.log("private_sendPB.formatPB: pPINs=" + JSON.stringify(pPINs));
    var _lPins = [];
    for (var _iO = 0; _iO < pPINs.length; _iO++)
    {
      var _lPin = {};
      var _lPBValues = null;
      if (pPINs[_iO] instanceof private_PINUpdate)
      {
        _lPBValues = private_sendPB.createPBValues(pPropDict, pPINs[_iO].mUpdatesPropVal, pPINs[_iO].mUpdatesExtras);
        _lPin.id = {id:pPINs[_iO].mPINAccessor().mPID, ident:0};
        _lPin.op = 'OP_UPDATE';
      }
      else if (typeof(pPINs[_iO]) == "object")
      {
        _lPBValues = private_sendPB.createPBValues(pPropDict, pPINs[_iO]);
        _lPin.op = 'OP_INSERT';
      }
      else throw "private_sendPB.formatPB: pPINs must be an array of PIN descriptions or PIN updates.";
      _lPin.values = _lPBValues; _lPin.nValues = _lPBValues.length;
      _lPin.rtt = (private_sendPB.isInsertingCollectionElements(_lPBValues) ? RT_PINS : RT_PIDS);
      _lPins.push(_lPin);
    }
    return _lPins;
  }
  // Static helper to serialize the descriptions/updates to a buffer in PB format, ready to be sent to mvstore.
  private_sendPB.serialize = function(pPINs, pPropDict, pFlush)
  {
    // REVIEW: Anything better than this gymnastic to control order?
    var _lMsg1 = {properties:pPropDict.getPBRepr()};
    var _lMsg2 = {pins:pPINs};
    var _lMsg3 = {};
    if (pFlush)
      _lMsg3.flush = [0];
    var _lSer1 = lMvStream.serialize(_lMsg1);
    var _lSer2 = lMvStream.serialize(_lMsg2);
    var _lSer3 = lMvStream.serialize(_lMsg3);
    var _lSer = new Buffer(_lSer1.length + _lSer2.length + _lSer3.length);
    _lSer1.copy(_lSer, 0);
    _lSer2.copy(_lSer, _lSer1.length);
    _lSer3.copy(_lSer, _lSer1.length + _lSer2.length);
    // Note:
    //   _lSer is a binary protobuf message ready to be sent to mvstore; it may look like this:
    //   {
    //     "pins":[{"op":"OP_INSERT","nValues":4,"values":[{"type":"VT_STRING","property":256,"str":"whatever","op":"OP_SET","eid":4294967295},{"type":"VT_DOUBLE","property":257,"d":123,"op":"OP_SET","eid":4294967295},{"type":"VT_DATETIME","property":258,"datetime":12956768270237000,"op":"OP_SET","eid":4294967295},{"type":"VT_ARRAY","property":259,"varray":{"l":4,"v":[{"type":"VT_DOUBLE","property":259,"d":1,"op":"OP_ADD","eid":4294967294},{"type":"VT_DOUBLE","property":259,"d":2,"op":"OP_ADD","eid":4294967294},{"type":"VT_DOUBLE","property":259,"d":3,"op":"OP_ADD","eid":4294967294},{"type":"VT_DOUBLE","property":259,"d":4,"op":"OP_ADD","eid":4294967294}]},"op":"OP_SET","eid":4294967295}],"rtt":"RT_PIDS"}],
    //     "properties":[{"str":"toto","id":256},{"str":"tata","id":257},{"str":"titi","id":258},{"str":"tutu","id":259}],
    //     "flush":[0]
    //   }
    // console.log("private_sendPB.serialize: returning " + JSON.stringify(lMvStream.parse(_lSer)));
    return _lSer;
  }
  // Static helper to determine whether we need RT_PINS or only RT_PIDS.
  // Note: Same logic as in mvstore.py.
  // REVIEW: I don't think mvstore offers a better option at the moment, but this might be prohibitively expensive in some cases.
  private_sendPB.isInsertingCollectionElements = function(pPBValues)
  {
    for (var _iV = 0; _iV < pPBValues.length; _iV++)
      { if (pPBValues[_iV].op == 'OP_ADD' || pPBValues[_iV].op == 'OP_ADD_BEFORE') return true; }
    return false;
  }
  // Static helper to convert an array of propvals (and optional extras) into their corresponding PB values.
  private_sendPB.createPBValues = function(pPropDict, pPropVals, pExtras/*optional*/)
  {
    pPropDict.newPIN(pPropVals);
    var _lPBValues = [];
    for (_iProp in pPropVals)
    {
      var _lPBValue = private_sendPB.createPBValue(pPropDict, _iProp, pPropVals[_iProp], (pExtras != undefined && _iProp in pExtras) ? pExtras[_iProp] : null);
      _lPBValues.push(_lPBValue);
    }
    return _lPBValues;
  }
  // Static helper to determine if a VT_* is a numeric type.
  private_sendPB.isNumericType = function(pType)
  {
    return ('VT_INT' == pType || 'VT_UINT' == pType || 'VT_INT64' == pType || 'VT_UINT64' == pType || 'VT_FLOAT' == pType || 'VT_DOUBLE' == pType || 'VT_DECIMAL' == pType);
  }
  // Static helper to convert a single propval into its corresponding PB value (MVStorePB.Value).
  private_sendPB.createPBValue = function(pPropDict, pKey, pValue, pExtra)
  {
    // TODO: Go over python/mvstore.py::_valuePY2PB and make sure everything is coverered...
    // Note: The caller may override some of the resulting fields (e.g. type, eid), based on its knowledge of 'extras'.
    var _lPropID = pPropDict.getID(pKey);
    // If the value is a collection, proceed.
    if (pValue instanceof Collection)
      { lib_assert.fail("private_sendPB.createPBValue: Unexpected Collection type encountered in createPBValue!"); return null; }
    else if (pValue instanceof Array)
    {
      if (undefined != pExtra && pExtra.length != pValue.length)
        { console.warn("WARNING (private_sendPB.createPBValue): " + pExtra.length + " extras for " + pValue.length + " elements in the collection!"); return null; }
      var _lValues = new Array();
      for (var _iE = 0; _iE < pValue.length; _iE++)
      {
        var _lV = private_sendPB.createPBValue(pPropDict, pKey, pValue[_iE], (undefined != pExtra) ? pExtra[_iE] : null);
        _lV.eid = EID_LAST_ELEMENT; _lV.op = 'OP_ADD';
        _lValues.push(_lV);
      }
      return {type:'VT_ARRAY', property:_lPropID, varray:{l:_lValues.length, v:_lValues}, eid:EID_COLLECTION, op:'OP_SET'};
    }
    // Otherwise, first determine the native value type.
    var _lType = 'VT_ANY';
    switch (typeof(pValue))
    {
      case "boolean": _lType = 'VT_BOOL'; break;
      case "number": _lType = 'VT_DOUBLE'; break;
      case "string": _lType = 'VT_STRING'; break;
      case "function": lib_assert.fail("private_sendPB.createPBValue: unexpected member of type 'function'!"); return null;
      case "object":
      {
        if (pValue instanceof Date) _lType = 'VT_DATETIME';
        else if (pValue instanceof Buffer) _lType = 'VT_BSTR';
        else if (pValue instanceof Url) _lType = 'VT_URL';
        else if (pValue instanceof Ref) _lType = pValue.getVT();
        else { lib_assert.fail("private_sendPB.createPBValue: unhandled object type for value " + JSON.stringify(pValue) + ":" + pValue.constructor); }
        break;
      }
    }
    // See if pExtra overrides the native value type, eid or op.
    // TODO: meta
    var _lEid = EID_COLLECTION;
    var _lOp = 'OP_SET';
    if (undefined != pExtra)
    {
      if ("eid" in pExtra)
        _lEid = pExtra.eid;
      if ("op" in pExtra)
        _lOp = pExtra.op;
      if ("type" in pExtra && pExtra.type != _lType)
      {
        switch (_lType)
        {
          case 'VT_DOUBLE':
            if (private_sendPB.isNumericType(pExtra.type))
              _lType = pExtra.type;
            break;
          // TODO: various flavors of VT_STRING etc.
          default:
            lib_assert.fail("private_sendPB.createPBValue: Unexpected VT override in pExtra: " + _lType + " overriden with " + pExtra.type);
            break;
        }
      }
    }
    // Produce the resulting value.
    var _lResult = {type:_lType, property:_lPropID, eid:_lEid, op:_lOp};
    switch (_lType)
    {
      case 'VT_BOOL': _lResult.b = pValue; break;
      case 'VT_INT': _lResult.i = pValue; break;
      case 'VT_UINT': _lResult.ui = pValue; break;
      case 'VT_INT64': _lResult.i64 = pValue; break;
      case 'VT_UINT64': _lResult.ui64 = pValue; break;
      case 'VT_FLOAT': _lResult.f = pValue; break;
      case 'VT_DOUBLE': _lResult.d = pValue; break;
      case 'VT_DECIMAL': _lResult.d = pValue; break; // ?
      case 'VT_URL': case 'VT_STRING': _lResult.str = pValue; break;
      case 'VT_BSTR': _lResult.bstr = pValue; break;
      case 'VT_DATETIME': _lResult.datetime = pValue.valueOf() * 1000 + MV_TIME_OFFSET; break;
      case 'VT_REFID': {var _lV = pValue.get(); _lResult.id = {id:_lV.id, ident:_lV.ident}; break;}
      case 'VT_REFIDPROP': {var _lV = pValue.get(); _lResult.ref = {id:{id:_lV.id, ident:_lV.ident}, property:pPropDict.newPropName(_lV.property)}; break;}
      case 'VT_REFIDELT': {var _lV = pValue.get(); _lResult.ref = {id:{id:_lV.id, ident:_lV.ident}, property:pPropDict.newPropName(_lV.property), eid:_lV.eid}; break;}
      default: lib_assert.fail("private_sendPB.createPBValue: Unhandled VT: " + _lType); return null;
    }
    return _lResult;
  }

  // Protobuf save completion: parse mvstore's response after inserting/updating PINs, and update the in-memory PINs accordingly.
  function private_PBResponseParser(_pPBResponse)
  {
    this.mParsed = lMvStream.parse(_pPBResponse);
    // Note:
    //   this.mParsed is a javascript object resulting from protobuf_for_node's parsing of the raw protobuf buffer
    //   returned by mvstore following an insert/update on existing PINs; it may represent only the updated parts of a PIN,
    //   and could look like this:
    //   {
    //     "pins":[{"id":{"id":327681},"mode":2147483648,"nValues":1,"values":[{"type":"VT_DOUBLE","property":259,"d":5}]}],
    //     "properties":[{"str":"tutu","id":259}],
    //     "result":{"count":1,"op":"OP_UPDATE"}
    //   }
    // console.log("private_PBResponseParser: this.mParsed=" + JSON.stringify(this.mParsed));
    this.mPropDict = new private_PropDict();
    this.mPropDict.registerProps(this.mParsed.properties);
  }
  private_PBResponseParser.prototype.finalizeCreates = function(pPINDescriptions)
  {
    if (this.mParsed.pins.length != pPINDescriptions.length)
      console.warn("WARNING (private_PBResponseParser.finalizeCreates): " + pPINDescriptions.length + " PINs were created, but mvstore's response contained " + this.mParsed.pins.length + " PINs.");
    var _lPIDs = [];
    var _lExtras = [];
    this._walk(
      function(_pPINIndex, _pPID) { _lPIDs.push(_pPID); },
      function(_pPINIndex, _pExtras) { _lExtras.push(_pExtras); });
    var lPINs = []
    for (var _iP = 0; _iP < _lPIDs.length; _iP++)
      lPINs.push(new PIN([_lPIDs[_iP], pPINDescriptions[_iP], _lExtras[_iP]]));
    return lPINs;
  }
  private_PBResponseParser.prototype.finalizeUpdates = function(pPINUpdates)
  {
    var _lPIDs = {};
    var _lPINs = [];
    var _iP;
    for (_iP = 0; _iP < pPINUpdates.length; _iP++)
      _lPIDs[pPINUpdates[_iP].getPID()] = pPINUpdates[_iP].getPIN();
    for (_iP in _lPIDs)
      _lPINs.push(_lPIDs[_iP]);
    //Interesting... when multiple updates are done on the same pin separately... review...
    //if (this.mParsed.pins.length != Object.keys(_lPIDs).length)
    //  console.warn("WARNING: " + Object.keys(_lPIDs).length + " PINs were modified, but response contained " + this.mParsed.pins.length + " PINs.");
    this._walk(
      function(_pPINIndex, _pPID)
      { 
        if (_pPINIndex >= pPINUpdates.length) return;
        if (pPINUpdates[_pPINIndex].getPID() != _pPID) console.warn("WARNING (private_PBResponseParser.finalizeUpdates.lambda1): PID mismatch - expected " + pPINUpdates[_pPINIndex].getPID() + " but obtained " + _pPID);
      },
      function(_pPINIndex, _pExtras)
      {
        if (_pPINIndex >= pPINUpdates.length) return;        
        var __lExtras = pPINUpdates[_pPINIndex].mPINAccessor().mExtras;
        for (var __iProp in _pExtras)
        {
          if (__iProp in __lExtras)
          {
            if (__lExtras[__iProp] instanceof Array)
            {
              if (__lExtras[__iProp].length != _pExtras[__iProp].length)
                console.warn("WARNING (private_PBResponseParser.finalizeUpdates.lambda2): collection length mismatch (" + __lExtras[__iProp].length + " vs " + _pExtras[__iProp].length + ")" );
              else for (var __iEl = 0; __iEl < _pExtras[__iProp].length; __iEl++)
                { private_PBResponseParser._transferExtras(__lExtras[__iProp][__iEl], _pExtras[__iProp][__iEl]); }
            }
            else
              { private_PBResponseParser._transferExtras(__lExtras[__iProp], _pExtras[__iProp]); }
          }
          else
            { __lExtras[__iProp] = _pExtras; }
        } 
      });
    return _lPINs;
  }
  private_PBResponseParser._transferExtras = function(pTo, pFrom)
  {
    pTo.eid = pFrom.eid;
    pTo.type = pFrom.type;
  }
  private_PBResponseParser.prototype._walk = function(pProcessPID, pProcessExtras)
  {
    // Walk each parsed pin.
    for (var _iP = 0; _iP < this.mParsed.pins.length; _iP++)
    {
      // Process the pid.
      var _lParsedPin = this.mParsed.pins[_iP];
      pProcessPID(_iP, _lParsedPin.id.id);

      // Process the eids.
      var _lExtras = {};
      for (var _iV = 0; _lParsedPin.values != undefined && _iV < _lParsedPin.values.length; _iV++)
      {
        var _lParsedValue = _lParsedPin.values[_iV];
        var _lPropName = this.mPropDict.getPropName(_lParsedValue.property);
        if ('VT_ARRAY' == _lParsedValue.type)
        {
          _lExtras[_lPropName] = [];
          for (var _iE = 0; _iE < _lParsedValue.varray.l; _iE++)
            _lExtras[_lPropName].push({eid:_lParsedValue.varray.v[_iE].eid, type:_lParsedValue.varray.v[_iE].type});
        }
        else
          _lExtras[_lPropName] = {eid:_lParsedValue.eid, type:_lParsedValue.type};
      }
      pProcessExtras(_iP, _lExtras);
    }
  }

  // Protobuf load: deserialize new PIN objects from their protobuf representation (obtained from mvstore, e.g. via pathsqlProto).
  function private_receivePB() {}
  private_receivePB.queryPINs = function(pPathSql, pCallback, pOptions)
  {
    var _lPD = new private_PropDict();
    var _lUrl = appendUrlOptions(lUri.pathname + "?q=" + encodeURIComponent(pPathSql) + "&i=pathsql&o=proto", pOptions);
    return private_http(
      _lUrl, null,
      function(_pE, _pR)
      {
        if (_pE) { pCallback(_pE, _pR); return; }
        pCallback(null, private_receivePB._createPINs(_pR, _lPD));
      });
  }
  private_receivePB.refreshPIN = function(pPINAccessor, pCallback)
  {
    var _lPD = new private_PropDict();
    return private_http(
      lUri.pathname + "?q=" + encodeURIComponent("SELECT * FROM @" + pPINAccessor().mPID.toString(16) + ";") + "&i=pathsql&o=proto", null,
      function(_pE, _pR)
      {
        if (_pE) { pCallback(_pE, _pR); return; }
        private_receivePB._refreshPINs(_pR, _lPD, [pPINAccessor]);
        pCallback(null, pPINAccessor().mPIN);
      });
  }
  private_receivePB._createPINs = function(pPB, pPropDict)
  {
    var _lPINs = new Array();
    var _lParsed = lMvStream.parse(pPB);
    // Note:
    //   _lParsed is a javascript object resulting from protobuf_for_node's parsing of the raw protobuf buffer
    //   returned by mvstore; it should represent a whole PIN (i.e. all of its properties), and should look like this:
    //   { 
    //     "owner":{"str":"","id":0},
    //     "pins":[{"id":{"id":327681},"nValues":4,"values":[{"type":"VT_STRING","property":256,"str":"whatever"},{"type":"VT_DOUBLE","property":257,"d":123},{"type":"VT_DATETIME","property":258,"datetime":12956767059615000},{"type":"VT_ARRAY","property":259,"varray":{"l":4,"v":[{"type":"VT_DOUBLE","d":1,"eid":0},{"type":"VT_DOUBLE","d":2,"eid":1},{"type":"VT_DOUBLE","d":3,"eid":2},{"type":"VT_DOUBLE","d":4,"eid":3}]}}]}],
    //     "properties":[{"str":"toto","id":256},{"str":"tata","id":257},{"str":"titi","id":258},{"str":"tutu","id":259}],
    //     "result":[{"count":1}]
    //   }
    // dbg_savePBStr(pPB);
    // console.log("private_receivePB._createPINs: _lParsed=" + JSON.stringify(_lParsed));
    pPropDict.registerProps(_lParsed.properties);
    if (undefined != _lParsed.pins) // Note: Could happen on some DELETE operations, for example.
    {
      for (var _iO = 0; _iO < _lParsed.pins.length; _iO++)
        _lPINs.push(new PIN(private_receivePB._extractPIN(_lParsed.pins[_iO], pPropDict)));
    }
    return _lPINs;
  }
  private_receivePB._refreshPINs = function(pPB, pPropDict, pPINAccessors)
  {
    var _lParsed = lMvStream.parse(pPB);
    // Note: See the note on _lParsed in private_receivePB._createPINs.
    // console.log("private_receivePB._refreshPINs: _lParsed=" + JSON.stringify(_lParsed));
    pPropDict.registerProps(_lParsed.properties);
    if (_lParsed.pins.length != pPINAccessors.length)
      console.warn("WARNING (private_receivePB._refreshPINs): " + pPINAccessors.length + " PINs were refreshed, but mvstore's response contained " + _lParsed.pins.length + " PINs.");
    for (var _iO = 0; _iO < _lParsed.pins.length; _iO++)
    {
      var _lPinAttrs = private_receivePB._extractPIN(_lParsed.pins[_iO], pPropDict);
      var _lPINa = pPINAccessors[_iO]();
      var _iPV, _iE;
      // Clear the old PIN.
      for (_iPV in _lPINa.mPropVals) { delete _lPINa.mPropVals[_iPV]; }
      for (_iE in _lPINa.mExtras) { delete _lPINa.mExtras[_iE]; }
      // Set its new attributes.
      for (_iPV in _lPinAttrs[1]) { _lPINa.mPropVals[_iPV] = _lPinAttrs[1][_iPV]; }
      for (var _iE in _lPinAttrs[2]) { _lPINa.mExtras[_iE] = _lPinAttrs[2][_iE]; }
    }
  }
  private_receivePB._extractPIN = function(pPBPIN, pPropDict)
  {
    // Convert pPBPIN, a js object that complies with the protobuf format (direct fruit of protobuf_for_node deserialization)
    // into the elements required to create an instance of our PIN class.
    var _lPropVals = {};
    var _lExtras = {};
    if ("values" in pPBPIN)
    {
      for (var _iProp = 0; _iProp < pPBPIN.values.length; _iProp++)
      {
        var _lPBValue = pPBPIN.values[_iProp];
        var _lPropName = pPropDict.getPropName(_lPBValue.property);
        _lPropVals[_lPropName] = private_receivePB._extractValue(_lPBValue, pPropDict);
        if ('VT_ARRAY' == _lPBValue.type)
        {
          _lExtras[_lPropName] = [];
          for (var _iE = 0; _iE < _lPBValue.varray.l; _iE++)
            { _lExtras[_lPropName].push({eid:_lPBValue.varray.v[_iE].eid, type:_lPBValue.varray.v[_iE].type}); }
        }
        else 
          { _lExtras[_lPropName] = {eid:_lPBValue.eid, type:_lPBValue.type}; }
      }
    }
    return [pPBPIN.id.id, _lPropVals, _lExtras];
  }
  private_receivePB._extractValue = function(pPBValue, pPropDict)
  {
    // TODO: Go over python/mvstore.py::_valuePB2PY and make sure everything is coverered...
    switch (pPBValue.type)
    {
      case 'VT_BOOL': return pPBValue.b;
      case 'VT_DOUBLE': return pPBValue.d;
      case 'VT_STRING': return pPBValue.str;
      case 'VT_URL': return new Url(pPBValue.str);
      case 'VT_BSTR': return pPBValue.bstr;
      case 'VT_INT': return pPBValue.i;
      case 'VT_UINT': return pPBValue.ui;
      case 'VT_INT64': return pPBValue.i64;
      case 'VT_UINT64': return pPBValue.ui64;
      case 'VT_FLOAT': return pPBValue.f;
      case 'VT_INTERVAL': return pPBValue.interval; // xxx
      case 'VT_ARRAY':
      {
        var _lValues = new Array()
        pPBValue.varray.v.forEach(function(_pEl) { Array.prototype.push.call(_lValues, private_receivePB._extractValue(_pEl)); });
        return _lValues;
      }
      case 'VT_DATETIME': return new Date((pPBValue.datetime - MV_TIME_OFFSET) / 1000);
      case 'VT_REFID': return new Ref(pPBValue.id.id, pPBValue.id.ident);
      case 'VT_REFIDPROP': return new Ref(pPBValue.ref.id.id, pPBValue.ref.id.ident, pPropDict.getPropName(pPBValue.ref.property));
      case 'VT_REFIDELT': return new Ref(pPBValue.ref.id.id, pPBValue.ref.id.ident, pPropDict.getPropName(pPBValue.ref.property), pPBValue.ref.eid);
      default: break;
    }
    return null;
  }

  function private_sanitizeSemicolon(pQ)
  {
    // Remove the last semicolon, if any, to make sure the store recognizes single-instructions as such (relevant for pathSQL->JSON only).
    if (undefined == pQ || 0 == pQ.length) return;
    for (var _i = pQ.length - 1; _i >= 0; _i--)
    {
      switch (pQ.charAt(_i))
      {
        case ";": return pQ.substr(0, _i);
        case " ": case "\n": continue;
        default: return pQ;
      }
    }
    return "";
  }

  /**
   * Public interface.
   */
  
  // Main, self-sufficient pathSQL interface (json output).
  // TODO: Maybe integrate a more automatic means of handling pagination (i.e. always paginate).
  function pathsql(_pPathSql, _pCallback, _pOptions)
  {
    if (private_TxCtx.inExplicitTx())
      console.warn("WARNING (pathsql): pathsql calls can't participate to protobuf transactions at the moment; use pathsqlProto instead.");
    return private_http(appendUrlOptions(lUri.pathname + "?q=" + encodeURIComponent(private_sanitizeSemicolon(_pPathSql)) + "&i=pathsql&o=json", _pOptions), null, _pCallback); 
  }
  function pathsqlCount(_pPathSql, _pCallback)
  {
    if (private_TxCtx.inExplicitTx())
      console.warn("WARNING (pathsqlCount): pathsqlCount calls can't participate to protobuf transactions at the moment; use pathsqlProto instead.");
    return private_http(lUri.pathname + "?q=" + encodeURIComponent(private_sanitizeSemicolon(_pPathSql)) + "&i=pathsql&o=json&type=count", null, _pCallback); 
  }

  // Auxiliary protobuf enhancers.
  // Notes:
  //   Motivations for these enhancers include the following:
  //   1. we have no json-in presently;
  //   2. js objects are natural carriers for PINs (e.g. query results);
  //   3. as shown by the python lib, this can reduce considerably the need for ORM translations.
  //   Perf remains to be analyzed in detail (protobuf_for_node, object copies etc.).
  //   I like the notion of separate 'PIN' objects (*not* meant to be the base-class of
  //   application's object structure), just like in C++ and python (it's unambiguous what needs to be saved etc.).
  function pathsqlProto(_pPathSql, _pCallback, _pOptions)
  {
    // Same as the 'pathsql' public service, except that here we go through protobuf, and we return PIN objects (see js class definition above)
    // back to _pCallback (instead of json). Could be preferred for perf reasons, or to avoid loss of information,
    // or to work with PIN objects in an end-to-end manner, or to work within the context of a protobuf transaction.

    // Implicit transaction: don't bother with txctx or long http request.
    if (!private_TxCtx.inExplicitTx())
      return private_receivePB.queryPINs(_pPathSql, _pCallback, _pOptions);

    // Within the context of an already running explicit transaction...
    var _lOpEnd = private_TxCtx.beginOp(private_TxCtx.makeHandler_onReceivePINs({txend:_pCallback}), {mustFlush:true});
    if (undefined != _lOpEnd)
    {
      lTxCtx.recordAny({stmt:[{sq:_pPathSql, cid:lNextCid++, rtt:'RT_PINS', offset:0, limit:9999/*REVIEW*/}]});
      _lOpEnd();
    }
    return null;
  }
  function createPINs(_pPINDescriptions, _pCallback) 
  {
    // _pPINDescriptions is expected to be an array of js object literals describing new PINs, e.g.
    // {prop1:value1, prop2:value2, prop3:[el1, el2, el3], etc.}; _pCallback is invoked with
    // the resulting array of PIN objects (see js class definition above).
    // Note: Unlike in python, _pPINDescriptions initially has no 'extras' specified (this is a pure PIN creation).

    // Implicit transaction: don't bother with txctx or long http request.
    if (!private_TxCtx.inExplicitTx())
      return private_sendPB(_pPINDescriptions, _pCallback, "createPINs");

    // Within the context of an already running explicit transaction...
    var _lOpEnd = private_TxCtx.beginOp(private_TxCtx.makeHandler_onNewPIDs(_pPINDescriptions, {txend:_pCallback}), {mustFlush:true});
    if (undefined != _lOpEnd)
    {
      var _lPD = new private_PropDict();
      var _lPins = private_sendPB.formatPB(_pPINDescriptions, _lPD);
      lTxCtx.recordAny([{properties:_lPD.getPBRepr()}, {pins:_lPins}]);
      _lOpEnd();
    }
    return null;
  }
  function startTx(_pTxLabel)
  {
    // Note: private_TxCtx takes care of unsetting itself from lTxCtx upon completion of an outermost transaction.
    if (!private_TxCtx.inExplicitTx())
      lTxCtx = new private_TxCtx();
    lTxCtx.startTx(_pTxLabel);
  }
  function commitTx(_pCallback)
  {
    if (!private_TxCtx.inExplicitTx())
      throw "commitTx: called outside of a transaction";
    lTxCtx.commitTx(_pCallback);
  }
  function rollbackTx(_pCallback)
  {
    if (!private_TxCtx.inExplicitTx())
      throw "rollbackTx: called outside of a transaction";
    lTxCtx.rollbackTx(_pCallback);
  }
  // ---
  // Other flavor of auxiliary protobuf enhancers: native js object persistence.
  function saveNativeJS(_pObjects, _pCallback)
  {
    // Note:
    //   We need to distinguish new objects from existing objects for a few reasons:
    //   1. until new objects are saved once, there's no other way to prevent saving the same objects multiple times due to multiple references pointing to them
    //   2. pathSQL/protobuf don't yet support persistence of graphs (with cycles) (but even if they did, we'd still have to go through this process for new objects)
    var _lThis = this;
    var _lPrototypes = []; // Set of unique prototypes involved (review; hash/set).
    var _lConstructors = []; // Set of unique constructors (+ "static" methods) involved.
    var _lToCreate = []; // Set of distinct instances that will need to be created at the end of the process.
    var _lToUpdate = []; // Set of distinct instances that will need to be updated at the end of the process.
    var _lPropDict = new private_PropDict(); // For protobuf serialization.
    
    // Define a bunch of internal services to deal with complexity.
    this.updateOrCreate = // Determine whether __pO should go in _lToCreate or in _lToUpdate, if not already there.
      function(__pO)
      {
        if ('pid' in __pO)
          _lToUpdate[__pO.pid] = __pO;
        else
        {
          var __lKnown = false;
          for (var __iChk = 0; __iChk < _lToCreate.length && !__lKnown; __iChk++) // review: hash...
            __lKnown = (_lToCreate[__iChk] === __pO);
          if (!__lKnown)
            _lToCreate.push(__pO);
        }
      };
    this.formatPBMethod =
      function(__pMethodName, __pMethodImpl, __pOut)
      {
        var __lMatch = __pMethodImpl.match(/function\s*(\w*)\s*\((.*)\)\s*\{(.*)\}/i);

        // Constructor name, if relevant.
        if (undefined != __lMatch[1] && __lMatch[1].length > 0)
        {
          _lPropDict.newPropName(PROP_JS_CONSTRUCTOR_NAME);
          __pOut.push(private_sendPB.createPBValue(_lPropDict, PROP_JS_CONSTRUCTOR_NAME, __lMatch[1], null));
        }

        // Method body.
        var __lPropName = (PREFIX_JS_METHOD + __pMethodName);
        _lPropDict.newPropName(__lPropName);
        __pOut.push(private_sendPB.createPBValue(_lPropDict, __lPropName, __lMatch[3], null));

        // Method parameters.
        __lPropName = (PREFIX_JS_PARAMETERS + __pMethodName);
        _lPropDict.newPropName(__lPropName);
        __pOut.push(private_sendPB.createPBValue(_lPropDict, __lPropName, __lMatch[2], null));
      };
    this.formatPB = // TODO (review): Try to reuse private_sendPB.formatPB... could attach a tmp persistence context to __pO for this...
      function(__pO, __pUseProto)
      {
        // Prepare each property.
        var __lPin = {};
        var __lPBValues = [];
        if (typeof(__pO) == "function")
          { _lThis.formatPBMethod('constructor', "" + __pO, __lPBValues); }
        for (var __iProp in __pO)
        {
          if (__iProp == 'pid') continue; // Review: Deal better with this.
          if (__iProp == PROP_JS_PROTOTYPE_HASH && __pUseProto) continue; // Review: Deal better with this.
          if (__iProp == PROP_JS_CONSTRUCTOR_HASH && __pUseProto) continue; // Review: Deal better with this.
          if (__iProp == PROP_JS_KIND && __pUseProto) continue; // Review: Deal better with this.
          var __lV = __pO[__iProp];
          if (typeof(__lV) == "function") // Persist functions as str (later, as expr).
          {
            // Only persist object-specific functions, if a known prototype is involved.
            if (__pUseProto)
            {
              var __lSkipFunc = false;
              for (var __iPf in __pO.constructor.prototype)
                if (__lV === __pO.constructor.prototype[__iPf]) { /*console.log("found known proto function: " + __iPf);*/ __lSkipFunc = true; break; }
              if (__lSkipFunc)
                continue;
            }
            _lThis.formatPBMethod(__iProp, "" + __lV, __lPBValues);
            continue;
          }
          else if (typeof(__lV) == "object")
          {
            if ('pid' in __lV) // Persist references as mvstore references.
              { __lV = new Ref(__lV.pid); }
            else if (__lV instanceof Array) // TODO: Nicer recursion. // TODO: Deal with empty arrays.
            {
              var __lPropName = (PREFIX_JS_MEMBER + __iProp);
              _lPropDict.newPropName(__lPropName);
              var __lCollValues = []
              for (var __iE = 0; __iE < __lV.length; __iE++)
              {
                var __lEV = __lV[__iE];
                if (typeof(__lEV) == "object" && 'pid' in __lEV)
                  { __lEV = new Ref(__lEV.pid); }
                var __lPBEV = private_sendPB.createPBValue(_lPropDict, __lPropName, __lEV, null);
                __lPBEV.eid = EID_LAST_ELEMENT; __lPBEV.op = 'OP_ADD';
                __lCollValues.push(__lPBEV);
              }
              __lPBValues.push({type:'VT_ARRAY', property:_lPropDict.getID(__lPropName), varray:{l:__lCollValues.length, v:__lCollValues}, eid:EID_COLLECTION, op:'OP_SET'});
              continue;
            }
          }
          // Default: plain data member.
          var __lPropName = (0 == __iProp.indexOf(PREFIX_JS_PROP)) ? __iProp : (PREFIX_JS_MEMBER + __iProp);
          _lPropDict.newPropName(__lPropName);
          var __lPBValue = private_sendPB.createPBValue(_lPropDict, __lPropName, __lV, null);
          __lPBValues.push(__lPBValue);
        }
        
        // Add properties for the prototype & constructor references.
        if (__pUseProto)
        {
          var __lPBValue = private_sendPB.createPBValue(_lPropDict, PROP_JS_PROTOTYPE, new Ref(__pO.constructor.prototype.pid), null);
          __lPBValues.push(__lPBValue);
          __lPBValue = private_sendPB.createPBValue(_lPropDict, PROP_JS_CONSTRUCTOR, new Ref(__pO.constructor.pid), null);
          __lPBValues.push(__lPBValue);
        }
        
        // Wrap up & return.
        // Note: Caller must set op='OP_INSERT'/'OP_UPDATE'...
        __lPin.values = __lPBValues; __lPin.nValues = __lPBValues.length;
        __lPin.rtt = RT_PIDS;
        return __lPin;
      }
    this.walkRefs =
      function(__pO, __pFunc, __pStack)
      {
        // Avoid infinite recursion in graphs.
        for (var iS = 0; undefined != __pStack && iS < __pStack.length; iS++)
          if (__pStack[iS] === __pO)
            return;
        var lStack = __pStack ? __pStack.concat([__pO]) : [__pO];
        
        // Walk each property of __pO.
        for (var iProp in __pO)
        {
          var lValue = __pO[iProp];
          switch (typeof(lValue))
          {
            // Skip all specific, known leaves.
            case "boolean": case "number": case "string": case "function": break;
            case "object":
            {
              // Some object types are known leaves.
              if (lValue instanceof Date) {}
              else if (lValue instanceof Buffer) {}
              // Walk arrays recursively.
              else if (lValue instanceof Array)
              {
                for (var iV = 0; iV < lValue.length; iV++)
                  _lThis.walkRefs(lValue[iV], __pFunc, lStack);
              }
              // Apply __pFunc to generic object leaves, and walk them recursively.
              // TODO: check a no-persistence marker.
              else
              {
                __pFunc(lValue);
                _lThis.walkRefs(lValue, __pFunc, lStack);
              }
              break;
            }
          }
        }
      };
    this.saveSpecial = function(__pSpecialObjects, __pSpecialProp, __pCallback)
    {
      // Compute a unique key (sha1 of the actual code), for all the prototypes/constructors that don't already have a key (not yet persisted).
      // Note: The purpose of this is to have a single instance of each distinct prototype/constructor, across the whole domain (including third-party components that may be introduced independently).
      // Note: The justification to have special handling here (compared with generic objects) is that presumably the app developer would not expect to have to manage his prototypes/constructors via queries...
      var lPropHash = (__pSpecialProp + "/hash");
      var lToSave = [];
      var lKeysToSave = [];
      for (var iSpecial = 0; iSpecial < __pSpecialObjects.length; iSpecial++)
      {
        // If this proto/constr is already registered, nothing to do (review: assumed immutable?).
        if ('pid' in __pSpecialObjects[iSpecial]) { continue; }
        if (lPropHash in __pSpecialObjects[iSpecial]) { alert("Unexpected!"); continue; }
        // Compute the proto/constr's hash (hash of all method names and implementations),
        // and see if it exists elsewhere in the db (if it does, add the pid here).
        var lSha = lib_crypto.createHash('sha1');
        if (typeof(__pSpecialObjects[iSpecial]) == "function")
          lSha.update("constructor:" + __pSpecialObjects[iSpecial]);
        for (var iMethod in __pSpecialObjects[iSpecial])
          lSha.update(iMethod + ":" + __pSpecialObjects[iSpecial][iMethod]);
        var lKey = lSha.digest('hex');
        __pSpecialObjects[iSpecial][PROP_JS_KIND] = __pSpecialProp;
        __pSpecialObjects[iSpecial][lPropHash] = "" + lKey;
        lToSave.push(__pSpecialObjects[iSpecial]);
        lKeysToSave.push("'" + lKey + "'");
      }

      // Query to see if by any chance the unknown prototypes are already in the db.
      // REVIEW: not 'from *' - have a class...
      // REVIEW: probably use pathsqlProto instead, to be all proto (tx etc.)...
      var lKeysToSaveStr = lKeysToSave.join(",");
      pathsql(
        "SELECT mv:pinID, \"" + lPropHash + "\" FROM * WHERE \"" + lPropHash + "\" IN (" + lKeysToSaveStr + ");",
        function(___pE, ___pR)
        {
          // For all prototypes found, update our in-memory instance with the pid, and remove from lToSave.
          // REVIEW: O(n*m)...
          for (var iFound = 0; undefined != ___pR && iFound < ___pR.length; iFound++)
          {
            for (var iS = 0; iS < lToSave.length; iS++)
            {
              if (lToSave[iS][lPropHash] == ___pR[iFound][lPropHash])
              {
                lToSave[iS].pid = parseInt(___pR[iFound].id);
                lToSave.splice(iS, 1);
                break;
              }
            }
          }

          // For all remaining (unsaved) prototypes, save them, and update our in-memory instance with the pid.
          if (lToSave.length > 0)
          {
            var lPBPins = [];
            for (var iSave = 0; iSave < lToSave.length; iSave++)
            {
              var lPBPin = _lThis.formatPB(lToSave[iSave], false);
              lPBPin.op = 'OP_INSERT';
              lPBPins.push(lPBPin);
            }
            var lMsgSer = private_sendPB.serialize(lPBPins, _lPropDict, true);
            var lOnSaved = function(___pE, ___pR) { for (var ___iP = 0; undefined != ___pR && ___iP < ___pR.length; ___iP++) { lToSave[___iP].pid = ___pR[___iP].pid; } __pCallback(); }
            private_sendPB.finalize(lToSave, lMsgSer, lOnSaved, "saveSpecial");
          }
        });
    }
    this.savePrototypes = function(__pCallback) { this.saveSpecial(_lPrototypes, PROP_JS_PROTOTYPE, __pCallback); }
    this.saveConstructors = function(__pCallback) { this.saveSpecial(_lConstructors, PROP_JS_CONSTRUCTOR, __pCallback); }
    this.saveCreates = function(__pCallback)
    {
      // As an interim cheap&simple solution until Mark's protobuf impl deals with cycles,
      // I'll pre-create all new objects empty, and then let the update path write everything;
      // this way, all referenced objects will assuredly exist.
      var lEmpty = [];
      for (var iC = 0; iC < _lToCreate.length; iC++)
        lEmpty.push({});
      createPINs(
        lEmpty,
        function(___pE, ___pR)
        {
          for (var ___iR = 0; ___iR < ___pR.length; ___iR++)
            _lToCreate[___iR].pid = ___pR[___iR].pid;
          _lToUpdate = _lToUpdate.concat(_lToCreate);
          __pCallback();
        });
    }
    this.saveUpdates = function(__pCallback)
    {
      // TODO: Deal with deleted fields that may have been removed by the user...
      // TODO: May want to adopt a more sturdy naming convention (maybe with a dynamic prefix also) to wrap native names (potentially very short, overlapping, etc.; at least don't run the chance to bother other apps/classes...)
      var lPBPins = [];
      for (var iU = 0; iU < _lToUpdate.length; iU++)
      {
        var lPBPin = _lThis.formatPB(_lToUpdate[iU], true);
        lPBPin.id = {id:_lToUpdate[iU].pid, ident:0};
        lPBPin.op = 'OP_UPDATE';
        lPBPins.push(lPBPin);
      }
      var lMsgSer = private_sendPB.serialize(lPBPins, _lPropDict, true);
      // console.log("saveUpdates: sending " + JSON.stringify(lMvStream.parse(lMsgSer)));
      private_http(lUri.pathname + "?i=proto&o=proto", lMsgSer,
        function(___pE, ___pR) { if (___pE) console.log("error: " + ___pE); /*else console.log("received:\n" + JSON.stringify(___pE));*/ __pCallback(); });
    }

    // Determine which objects must be created; gather all references (may point to objects not explicitly in _pObjects).
    for (var iObj = 0; iObj < _pObjects.length; iObj++)
    {
      this.updateOrCreate(_pObjects[iObj]);
      this.walkRefs(_pObjects[iObj], this.updateOrCreate);
    }

    // Collect all distinct constructors.
    var _lAllObjects = _lToCreate.concat(_lToUpdate);
    for (var iObj = 0; iObj < _lAllObjects.length; iObj++)
    {
      var lSkip = false;
      for (var iConstr = 0; iConstr < _lConstructors.length && !lSkip; iConstr++)
        { lSkip = (_lConstructors[iConstr] === _lAllObjects[iObj].constructor); }
      if (!lSkip)
        { _lConstructors.push(_pObjects[iObj].constructor); }
    }

    // Collect all distinct prototypes.
    for (var iObj = 0; iObj < _lAllObjects.length; iObj++)
    {
      var lSkip = false;
      for (var iProto = 0; iProto < _lPrototypes.length && !lSkip; iProto++)
        { lSkip = (_lPrototypes[iProto] === _lAllObjects[iObj].constructor.prototype); }
      if (!lSkip)
        { _lPrototypes.push(_lAllObjects[iObj].constructor.prototype); }
    }

    // Save everything.
    // TODO: add pb transaction around all this.
    _lPropDict.newPropName(PROP_JS_PROTOTYPE);
    _lPropDict.newPropName(PROP_JS_CONSTRUCTOR);
    _lThis.savePrototypes(function() { _lThis.saveConstructors(function() { _lThis.saveCreates(function() { _lThis.saveUpdates(_pCallback); }); }); });
  }
  function loadNativeJS(_pPathSql, _pCallback)
  {
    // TODO:
    // . use protobuf
    // . memory and references management (recurse references, connect everything etc.)
    // . batching (e.g. get all protos/constr at once)
    var lObjProcessor =
      function(__pObj, __pResult, __pSS)
      {
        var _lF =
          function()
          {
            pathsql(
              "SELECT * WHERE @ IN (@" + __pObj[PROP_JS_PROTOTYPE]["$ref"] + ",@" + __pObj[PROP_JS_CONSTRUCTOR]["$ref"] + ")",
              function(___pE, ___pR)
              {
                if (___pE) { _pCallback(___pE, null); return; }

                // Build the constructor&prototype.
                var lConstr = new Function("", ___pR[1][PREFIX_JS_METHOD + 'constructor']);
                lConstr.prototype = {};
                lConstr.prototype.constructor = lConstr;
                for (var ___iProp in ___pR[0])
                {
                  if (0 != ___iProp.indexOf(PREFIX_JS_METHOD)) continue;
                  var ___lMethodName = ___iProp.substr(PREFIX_JS_METHOD.length);
                  var ___lMethodImpl = ___pR[0][___iProp];
                  var ___lMethodParams = ___pR[0][PREFIX_JS_PARAMETERS + ___lMethodName];
                  lConstr.prototype[___lMethodName] = new Function(___lMethodParams, ___lMethodImpl);
                }

                // Create the new object and fill it with its values.
                var lObj = new lConstr();
                __pResult.push(lObj);
                for (var ___iProp in __pObj)
                {
                  if (0 != ___iProp.indexOf(PREFIX_JS_MEMBER)) continue;
                  var ___lMemberName = ___iProp.substr(PREFIX_JS_MEMBER.length);
                  lObj[___lMemberName] = __pObj[___iProp];
                }
                
                __pSS.next();
              });
          };
        return _lF;
      };
    pathsql(
      _pPathSql,
      function(__pE, __pR)
      {
        if (__pE) { _pCallback(__pE, null); return; }
        var _lResult = [];
        var _lSS = new module.exports.instrSeq();
        for (var __iP = 0; __iP < __pR.length; __iP++)
        {
          var __lP = __pR[__iP];
          if (PROP_JS_PROTOTYPE in __lP)
            _lSS.push(lObjProcessor(__lP, _lResult, _lSS));
        }
        _lSS.push(function() { _pCallback(null, _lResult); });
        _lSS.start();
      });
  }
  // ---
  function terminate()
  {
    if (lKeepAlive.on && undefined != lKeepAlive.client)
    {
      lKeepAlive.client.destroy();
      lKeepAlive.client = null;
    }
  }

  /**
   * Publish the public interface.
   */
  var lConnection =
  {
    rawGet:function(_pCallback) {private_http("", null, _pCallback);},
    keptAlive:function() {return lKeepAlive.on;},
    q: pathsql, qCount: pathsqlCount,
    qProto:pathsqlProto, createPINs:createPINs,
    saveNativeJS:saveNativeJS, loadNativeJS:loadNativeJS, // WARNING: Only a proof of concept for the moment.
    startTx:startTx, commitTx:commitTx, rollbackTx:rollbackTx,
    makeUrl:function(_pString) {return new Url(_pString);},
    makeRef:function(_pLocalPID, _pIdent, _pProperty, _pEid) {return new Ref(_pLocalPID, _pIdent, _pProperty, _pEid);},
    terminate:terminate,
  };
  return lConnection;
}

// TODO: add a guard to the public interface, to warn against attempting to do concurrent things in a connection
// TODO: in keepalive mode, should better integrate pure pathSQL transactions with startTx-commitTx (Ming's argument).
