/*******************************************************************************
 *  Code contributed to the webinos project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Copyright 2013 Sony Mobile Communications
 *
 ******************************************************************************/
(function () {
  var RPCWebinosService = require('webinos-jsonrpc2').RPCWebinosService;
  var CKANAdapterImpl = require("./ckanAdapter_impl");

  var CKANAdapterModule = function (rpcHandler, params) {
    this.rpcHandler = rpcHandler;
    this.params = params;
    this.internalRegistry = {};
  };

  CKANAdapterModule.prototype.init = function (register, unregister) {
    this.register = register;
    this.unregister = unregister;
  };

  CKANAdapterModule.prototype.updateServiceParams = function (serviceId, params) {
    var self = this,
      id;

    if (serviceId && self.internalRegistry[serviceId]) {
      self.unregister({"id":serviceId, "api": self.internalRegistry[serviceId].api} );
      delete self.internalRegistry[serviceId];
    }

    if (params) {
      var service = new CKANAdapterService(this.rpcHandler, params);
      id = this.register(service);
      this.internalRegistry[id] = service;
    }

    return id;
  };

  var CKANAdapterService = function (rpcHandler, params) {
    // inherit from RPCWebinosService
    this.base = RPCWebinosService;
    this.base({
//      api: 'http://ubiapps.com/api/ckanadapter/' + params.format,
      api: 'http://ubiapps.com/api/ckanadapter',
      displayName: params.resourceName + " [" + params.format + "]",
      description: params.resourceName
    });

    this.rpcHandler = rpcHandler;

    this._impl = new CKANAdapterImpl(params);
  };

  CKANAdapterService.prototype = new RPCWebinosService;

  CKANAdapterService.prototype.getFormat = function (params, successCB, errorCB) {
    return this._impl.getFormat(successCB, errorCB);
  };

  CKANAdapterService.prototype.canBrowse = function (params, successCB, errorCB) {
    return this._impl.canBrowse(successCB, errorCB);
  };

  CKANAdapterService.prototype.getSchema = function (params, successCB, errorCB) {
    return this._impl.getSchema(params, successCB, errorCB);
  };

  CKANAdapterService.prototype.getRows = function(params, successCB, errorCB) {
    return this._impl.getRows(params[0],successCB, errorCB);
  };

  CKANAdapterService.prototype.getRowCount = function(params, successCB, errorCB) {
    return this._impl.getRowCount(params[0], successCB, errorCB);
  };

  CKANAdapterService.prototype.getDownloadUrl = function (params, successCB, errorCB) {
    return this._impl.getDownloadUrl(successCB, errorCB);
  };

  // export our object
  exports.Module = CKANAdapterModule;
})();
