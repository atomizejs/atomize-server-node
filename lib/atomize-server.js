/*global require, exports */
/*jslint devel: true */

(function () {
    'use strict';

    var atomize = require('atomize-client'),
        cereal = require('cereal'),
        sockjs = require('sockjs'),
        events = require('events'),
        sockjs_opts = {sockjs_url: "http://cdn.sockjs.org/sockjs-0.1.min.js",
                       log: function () {}},
        globalTVarCount = 0,
        globalTVars = {},
        globalLocal = {},
        create, util, rootTVar;

    util = (function () {
        return {
            isPrimitive: function (obj) {
                return obj !== Object(obj);
            },

            hasOwnProp: ({}).hasOwnProperty,

            shallowCopy: function (src, dest) {
                var keys = Object.keys(src),
                    i;
                for (i = 0; i < keys.length; i += 1) {
                    dest[keys[i]] = src[keys[i]];
                }
            },

            lift: function (src, dest, fields) {
                var i;
                for (i = 0; i < fields.length; i += 1) {
                    dest[fields[i]] = src[fields[i]].bind(src);
                }
            }
        };
    }());

    function TVar(id, isArray, raw, creator) {
        this.creator = creator;
        this.raw = isArray ? [] : {};
        util.shallowCopy(raw, this.raw);
        this.isArray = isArray;
        this.id = id;
        this.version = 0;
        this.observers = [];
    }
    TVar.prototype = {
        subscribe: function (fun) {
            this.observers.push(fun);
        },

        bump: function (future) {
            // create a fresh observers array otherwise, if on the
            // server side firing the observer creates a new
            // subscription, you can go into a loop!
            var observers = this.observers,
                i;
            this.version += 1;
            this.observers = [];
            for (i = 0; i < observers.length; i += 1) {
                (observers[i])(this, future);
            }
        }
    };

    TVar.create = function (isArray, value, creator) {
        var globalTVar;
        globalTVarCount += 1;
        globalTVar = new TVar(globalTVarCount, isArray, value, creator);
        globalTVars[globalTVar.id] = globalTVar;
        return globalTVar;
    };

    function CoalescingObserver() {
        this.map = new cereal.Map();
        this.keys = [];
    }
    CoalescingObserver.prototype = {
        insert: function (key, value) {
            if (!this.map.has(key)) {
                this.keys.push(key);
            }
            this.map.set(key, value);
        },

        force: function () {
            var i;
            for (i = 0; i < this.keys.length; i += 1) {
                (this.map.get(this.keys[i]))();
            }
            this.keys = [];
            this.map = new cereal.Map();
        }
    }

    function Client(connection, serverEventEmitter) {
        this.emitter = new events.EventEmitter();
        util.lift(this.emitter, this,
                  ['on', 'once', 'removeListener', 'removeAllListeners', 'emit']);

        this.connection = connection;
        this.localGlobal = {};
        this.localGlobal[rootTVar.id] = rootTVar.id;
        this.globalLocal = {};
        this.globalLocal[rootTVar.id] = rootTVar.id;
        globalLocal[connection.id] = this.globalLocal;
        this.minTVarId = 0;

        connection.on('data', this.data.bind(this));
        connection.on('close', this.close.bind(this));
        serverEventEmitter.emit('connection', this);
    }

    Client.prototype = {
        isAuthenticated: false,

        data: function (message) {
            if (this.isAuthenticated) {
                this.dispatch(message);
            } else {
                this.emit('data', message, this);
            }
        },

        close: function () {
            this.emit('close', this);
            delete globalLocal[this.connection.id];
        },

        write: function (msg) {
            this.connection.write(cereal.stringify(msg));
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
            var keys = Object.keys(txnLog.created),
                ok = true,
                i, localTVar, globalTVar;

            for (i = 0; i < keys.length; i += 1) {
                localTVar = txnLog.created[keys[i]];
                localTVar.id = keys[i];

                globalTVar = TVar.create(localTVar.isArray, localTVar.value, this);
                this.localGlobal[localTVar.id] = globalTVar.id;
                this.globalLocal[globalTVar.id] = localTVar.id;
            }

            return ok;
        },

        checkThing: function (thing, updates) {
            var keys = Object.keys(thing),
                ok = true,
                i, localTVar, globalTVar;

            for (i = 0; i < keys.length; i += 1) {
                localTVar = thing[keys[i]];
                localTVar.id = keys[i];
                globalTVar = globalTVars[this.localGlobal[localTVar.id]];
                if (localTVar.version !== globalTVar.version) {
                    updates[globalTVar.id] = true;
                    ok = false;
                }
            }

            return ok;
        },

        checkReads: function (txnLog, updates) {
            return this.checkThing(txnLog.read, updates);
        },

        checkWrites: function (txnLog, updates) {
            return this.checkThing(txnLog.written, updates);
        },

        retry: function (txnLog) {
            var txnId = txnLog.txnId,
                updates = {},
                ok, self, fired, observer, keys, i, localTVar, globalTVar;

            ok = this.checkReads(txnLog, updates);

            if (ok) {
                self = this;
                fired = false;
                observer = function (globalTVar, future) {
                    updates[globalTVar.id] = true;
                    future.insert(
                        updates,
                        function () {
                            if (!fired) {
                                fired = true;
                                self.write(self.createUpdates(updates, txnId));
                                self.write({type: "retry", txnId: txnId});
                            }
                        });
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
            var txnId = txnLog.txnId,
                updates = {},
                ok, keys, i, j, localTVar, globalTVar, future, names, name, value;

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

            if (ok && this.onPreCommit(txnLog)) {
                // send out the success message first. This a)
                // improves performance; and more importantly b)
                // ensures that any updates that end up being sent to
                // the same client (due to pending retries) get
                // received after the success msg and thus we don't
                // fall out of step with vsns
                this.write({type: "commit", txnId: txnId, result: "success"});

                future = new CoalescingObserver();

                keys = Object.keys(txnLog.written);
                for (i = 0; i < keys.length; i += 1) {
                    localTVar = txnLog.written[keys[i]];
                    localTVar.id = keys[i];
                    globalTVar = globalTVars[this.localGlobal[localTVar.id]];
                    names = Object.keys(localTVar.children);
                    for (j = 0; j < names.length; j += 1) {
                        name = names[j];
                        value = localTVar.children[name];
                        if (undefined !== value.tvar) {
                            value.value = {tvar: this.localGlobal[value.tvar]};
                            delete value.tvar;
                            Object.defineProperty(globalTVar.raw, name, value);
                        } else if (undefined !== value.deleted) {
                            delete globalTVar.raw[name];
                        } else {
                            // mess for dealing with arrays, and some proxy stuff
                            if (globalTVar.isArray && 'length' === name) {
                                globalTVar.raw[name] = value.value;
                            } else {
                                Object.defineProperty(globalTVar.raw, name, value);
                            }
                        }
                    }
                    globalTVar.bump(future);
                }

                future.force();
            } else {
                this.write(this.createUpdates(updates, txnId));
                this.write({type: "commit", txnId: txnId, result: "failure"});
            }
        },

        onPreCommit: function (txnLog) {
            return true;
        },

        createUpdates: function (updates, txnId) {
            var keys = Object.keys(updates),
                txnLog = {type: "updates", updates: {}, txnId: txnId},
                key, globalTVar, localTVar, names, name, i, desc;

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

                localTVar.value = {};
                localTVar.isArray = globalTVar.raw.constructor === Array;

                names = Object.getOwnPropertyNames(globalTVar.raw);
                for (i = 0; i < names.length; i += 1) {
                    name = names[i];
                    desc = Object.getOwnPropertyDescriptor(globalTVar.raw, name);
                    if (util.hasOwnProp.call(desc.value, 'tvar')) {
                        localTVar.value[name] = desc;
                        if (undefined === this.globalLocal[desc.value.tvar]) {
                            this.minTVarId -= 1;
                            this.localGlobal[this.minTVarId] = desc.value.tvar;
                            this.globalLocal[desc.value.tvar] = this.minTVarId;
                            keys.push(desc.value.tvar);
                            desc.tvar = this.minTVarId;
                        } else {
                            desc.tvar = this.globalLocal[desc.value.tvar];
                        }
                        delete desc.value;
                    } else {
                        localTVar.value[name] = desc;
                    }
                }
            }
            return txnLog;
        }
    };

    function ServerEventEmitter () {
        this.emitter = new events.EventEmitter();
        util.lift(this.emitter, this,
                  ['on', 'once', 'removeListener', 'removeAllListeners', 'emit']);
        this.on('connection', this.defaultAuthenticator.bind(this));
    }

    ServerEventEmitter.prototype = {
        connection: function (connection) {
            new Client(connection, this);
        },

        defaultAuthenticator: function (client) {
            if (1 === this.emitter.listeners('connection').length) {
                // nothing installed other than us, just let it straight through!
                client.isAuthenticated = true;
            }
        }
    };

    create = function (http, path, root) {
        var server = sockjs.createServer(sockjs_opts),
            emitter = new ServerEventEmitter();

        server.on('connection', emitter.connection.bind(emitter));
        http.addListener('upgrade', function (req, res) { res.end(); });
        server.installHandlers(http, {prefix: path});

        if (undefined === root || null === root) {
            root = {};
        }
        globalTVarCount = 1;
        rootTVar = new TVar(globalTVarCount, Array.isArray(root), root, undefined);
        globalTVars[rootTVar.id] = rootTVar;
        rootTVar.bump(); // Need 2 bumps to ensure we're ahead of any clients.
        rootTVar.bump();

        emitter.client = function () { return new atomize.Atomize(Client, emitter); };

        return emitter;
    };

    exports.create = create;
}());
