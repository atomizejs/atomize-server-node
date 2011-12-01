/*global require, exports */
/*jslint devel: true */

(function () {
    'use strict';

    var atomize, cereal, http, sockjs, sockjs_opts, port, globalTVarCount, globalTVars, globalLocal, create;

    cereal = require('cereal');
    atomize = require('atomize-client');
    sockjs = require('sockjs');

    sockjs_opts = {sockjs_url: "http://cdn.sockjs.org/sockjs-0.1.min.js"};
    port = 9999; // Default port: override with --port command line option

    function TVar(id, raw) {
        this.raw = raw;
        this.id = id;
        this.version = 0;
        this.observers = [];
    }
    TVar.prototype = {
        subscribe: function (fun) {
            this.observers.push(fun);
        },

        bump: function () {
            var i, observers;
            // create a fresh observers array otherwise, if on the
            // server side firing the observer creates a new
            // subscription, you can go into a loop!
            this.version += 1;
            observers = this.observers;
            this.observers = [];
            for (i = 0; i < observers.length; i += 1) {
                (observers[i])(this);
            }
        }
    };


    TVar.create = function (value) {
        var globalTVar;
        globalTVarCount += 1;
        globalTVar = new TVar(globalTVarCount, value);
        globalTVars[globalTVar.id] = globalTVar;
        return globalTVar;
    };

    globalTVarCount = 1;
    globalTVars = {1: new TVar(1, {})};
    globalTVars['1'].version = 1;
    globalLocal = {};

    function Client(conn) {
        this.conn = conn;
        this.localGlobal = {1: 1};
        this.globalLocal = {1: 1};
        globalLocal[conn.id] = this.globalLocal;
        this.minTVarId = 0;
    }

    Client.prototype = {
        write: function (msg) {
            this.conn.write(cereal.stringify(msg));
        },

        read: function (msg) {
            return cereal.parse(msg);
        },

        dispatch: function (msg) {
            var txnLog = this.read(msg);
            switch (txnLog.type) {
            case "commit":
                this.commit(txnLog);
                break;
            case "retry":
                this.retry(txnLog);
                break;
            default:
                console.log("Received unexpected message from client:");
                console.log(txnLog);
            }
        },

        create: function (txnLog, updates) {
            var keys, i, ok, localTVar, globalTVar;
            ok = true;
            keys = Object.keys(txnLog.created);

            for (i = 0; i < keys.length; i += 1) {
                localTVar = txnLog.created[keys[i]];
                localTVar.id = keys[i];

                globalTVar = TVar.create(localTVar.value);
                this.localGlobal[localTVar.id] = globalTVar.id;
                this.globalLocal[globalTVar.id] = localTVar.id;
            }

            return ok;
        },

        checkReads: function (txnLog, updates) {
            var keys, i, ok, localTVar, globalTVar;
            ok = true;

            keys = Object.keys(txnLog.read);
            for (i = 0; i < keys.length; i += 1) {
                localTVar = txnLog.read[keys[i]];
                localTVar.id = keys[i];
                globalTVar = globalTVars[this.localGlobal[localTVar.id]];
                if (localTVar.version !== globalTVar.version) {
                    updates[globalTVar.id] = true;
                    ok = false;
                }
            }

            return ok;
        },

        checkWrites: function (txnLog, updates) {
            var keys, i, ok, localTVar, globalTVar;
            ok = true;

            keys = Object.keys(txnLog.written);
            for (i = 0; i < keys.length; i += 1) {
                localTVar = txnLog.written[keys[i]];
                localTVar.id = keys[i];
                globalTVar = globalTVars[this.localGlobal[localTVar.id]];
                if (localTVar.version !== globalTVar.version) {
                    updates[globalTVar.id] = true;
                    ok = false;
                }
            }

            return ok;
        },

        retry: function (txnLog) {
            var txnId, ok, updates, keys, i, localTVar, globalTVar, self, observer, fired;

            txnId = txnLog.txnId;
            updates = {};

            ok = this.checkReads(txnLog, updates);

            if (ok) {
                self = this;
                fired = false;
                observer = function (globalTVar) {
                    if (! fired) {
                        fired = true;
                        updates[globalTVar.id] = true;
                        self.write(self.createUpdates(updates, txnId));
                        self.write({type: "retry", txnId: txnId});
                    }
                };

                keys = Object.keys(txnLog.read);
                for (i = 0; i < keys.length; i += 1) {
                    localTVar = txnLog.read[keys[i]];
                    localTVar.id = keys[i];
                    globalTVar = globalTVars[this.localGlobal[localTVar.id]];
                    globalTVar.subscribe(observer);
                }
            } else {
                // oh good, need to do updates already and can then
                // immediately restart the txn.
                this.write(this.createUpdates(updates, txnId));
                this.write({type: "retry", txnId: txnId});
            }

        },

        commit: function (txnLog) {
            var txnId, ok, i, j, keys, localTVar, globalTVar, names, name, value, updates, key;

            txnId = txnLog.txnId;
            updates = {};

            // Prevent a short-cut impl of && from causing us problems
            ok = this.create(txnLog, updates);
            ok = this.checkReads(txnLog, updates) && ok;
            ok = this.checkWrites(txnLog, updates) && ok;

            // Regardless of success, we have grabbed the creates. We
            // should now bump their versions from 0 to 1.
            keys = Object.keys(txnLog.created);
            for (i = 0; i < keys.length; i += 1) {
                localTVar = txnLog.created[keys[i]];
                localTVar.id = keys[i];
                globalTVar = globalTVars[this.localGlobal[localTVar.id]];
                globalTVar.bump();
            }

            if (ok) {
                // send out the success message first. This a)
                // improves performance; and more importantly b)
                // ensures that any updates that end up being sent to
                // the same client (due to pending retries) get
                // received after the success msg and thus we don't
                // fall out of step with vsns
                this.write({type: "commit", txnId: txnId, result: "success"});

                keys = Object.keys(txnLog.written);
                for (i = 0; i < keys.length; i += 1) {
                    localTVar = txnLog.written[keys[i]];
                    localTVar.id = keys[i];
                    globalTVar = globalTVars[this.localGlobal[localTVar.id]];
                    names = Object.keys(localTVar.children);
                    for (j = 0; j < names.length; j += 1) {
                        name = names[j];
                        value = localTVar.children[name];
                        if (undefined !== value.primitive) {
                            globalTVar.raw[name] = value;
                        } else if (undefined !== value.tvar) {
                            globalTVar.raw[name] = {tvar: this.localGlobal[value.tvar]};
                        } else if (undefined !== value.deleted) {
                            delete globalTVar.raw[name];
                        }
                    }
                    globalTVar.bump();
                }

            } else {
                this.write(this.createUpdates(updates, txnId));
                this.write({type: "commit", txnId: txnId, result: "failure"});
            }
        },

        createUpdates: function (updates, txnId) {
            var keys, key, txnLog, localTVar, globalTVar, names, name, j, value;
            keys = Object.keys(updates);
            txnLog = {type: "updates", updates: {}, txnId: txnId};
            while (0 < keys.length) {
                key = keys.shift();
                globalTVar = globalTVars[key];
                localTVar = {version: globalTVar.version};

                if (undefined === this.globalLocal[globalTVar.id]) {
                    this.minTVarId -= 1;
                    txnLog.updates[this.minTVarId] = localTVar;
                    this.localGlobal[this.minTVarId] = globalTVar.id;
                    this.globalLocal[globalTVar.id] = this.minTVarId;
                } else {
                    txnLog.updates[this.globalLocal[globalTVar.id]] = localTVar;
                }

                // TODO: check on array objects for additional
                // manually added properties.

                if (globalTVar.raw.constructor === Array) {
                    localTVar.value = [];
                    for (j = 0; j < globalTVar.raw.length; j += 1) {
                        value = globalTVar.raw[j];
                        if (undefined === value.tvar && undefined === value.primitive) {
                            localTVar.value[j] = {primitive: value};
                        } else if (undefined === value.tvar) {
                            // must be primitive
                            localTVar.value[j] = value;
                        } else {
                            if (undefined === this.globalLocal[value.tvar]) {
                                this.minTVarId -= 1;
                                this.localGlobal[this.minTVarId] = value.tvar;
                                this.globalLocal[value.tvar] = this.minTVarId;
                                keys.push(value.tvar);
                                localTVar.value[j] = {tvar: this.minTVarId};
                            } else {
                                localTVar.value[j] = {tvar: this.globalLocal[value.tvar]};
                            }
                        }
                    }

                } else {
                    localTVar.value = {};
                    names = Object.getOwnPropertyNames(globalTVar.raw);
                    for (j = 0; j < names.length; j += 1) {
                        name = names[j];
                        value = globalTVar.raw[name];
                        if (undefined === value.tvar && undefined === value.primitive) {
                            localTVar.value[name] = {primitive: value};
                        } else if (undefined === value.tvar) {
                            // must be primitive
                            localTVar.value[name] = value;
                        } else {
                            if (undefined === this.globalLocal[value.tvar]) {
                                this.minTVarId -= 1;
                                this.localGlobal[this.minTVarId] = value.tvar;
                                this.globalLocal[value.tvar] = this.minTVarId;
                                keys.push(value.tvar);
                                localTVar.value[name] = {tvar: this.minTVarId};
                            } else {
                                localTVar.value[name] = {tvar: this.globalLocal[value.tvar]};
                            }
                        }
                    }
                }
            }
            return txnLog;
        }
    };

    create = function (http, path) {
        var server = sockjs.createServer({sockjs_url: "http://cdn.sockjs.org/sockjs-0.1.min.js"});
        server.on('connection', function (conn) {

            var client = new Client(conn);

            conn.on('data', function (message) {
                client.dispatch(message);
            });
        });

        http.addListener('upgrade', function (req, res) { res.end(); });
        server.installHandlers(http, {prefix: path});
    };

    exports.create = create;
    exports.atomize = new atomize.Atomize(Client);
}());
