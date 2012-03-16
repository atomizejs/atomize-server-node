/*global require, exports */
/*jslint devel: true */

/* ***************************************************************************
# AtomizeJS Node Server

Distributed objects are represented by TVar objects. When an object is
created, its value is passed directly into the TVar constructor which
takes a copy of it, and sets up version tracking and observer
patterns.  TVars have global IDs, which are not communicated to
clients, and client-local IDs, which are. Thus each client maintains a
mapping from client-local IDs to global IDs, and vice versa. Thus you
can think of global IDs as being abstract physical addresses, and
client-local IDs as being abstract virtual addresses, if you want to
think in OS terms.

Updates to an object's fields appear as property descriptors within
the transaction log, received from clients. If the descriptor object
has a 'tvar' field, then the descriptor's 'value' field is set to be
the corresponding global TVar ID. Thus the TVar's 'raw' object will
contain fields with abstract pointers of the form {'tvar': id}. If the
field's value is a primitive, then the value is applied directly to
the TVar's 'raw' object, modulo some rubbish for coping with the fact
that for some daft reason you can't do Object.defineProperty on an
Array's 'length' field. Fields which have been deleted by clients are
indicated as such in the transaction log received from the client and
get directly deleted from the TVar's 'raw' object.

When updates are sent down to clients, we send down the entire object,
with each of its existing fields as a property descriptor. The client
is responsible for updating its own representation of the object
accordingly - in particular identifying which, if any fields, have
been deleted. Again, if a field's value is an abstract pointer of the
form {'tvar': id} then the descriptor passed to the client will have
no 'value' field, but instead a 'tvar' field, the value of which is
the client-local ID.

*  ***************************************************************************/

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
        this.globalLocal = {};
        globalLocal[connection.id] = this.globalLocal;
        this.minTVarId = 0;

        connection.on('data', this.data.bind(this));
        connection.on('close', this.close.bind(this));
        serverEventEmitter.emit('connection', this);

        this.addToMapping(rootTVar.id, rootTVar.id);
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

        addToMapping: function (globalTVarId, localTVarId) {
            if (undefined === localTVarId) {
                this.minTVarId -= 1;
                localTVarId = this.minTVarId;
            }
            this.localGlobal[localTVarId] = {id: globalTVarId, version: 0};
            this.globalLocal[globalTVarId] = localTVarId;
            return localTVarId;
        },

        toGlobalTVar: function (localTVarId) {
            return globalTVars[this.localGlobal[localTVarId].id];
        },

        toGlobalTVarID: function (localTVarId) {
            return this.localGlobal[localTVarId].id;
        },

        toLocalTVarID: function (globalTVarId) {
            return this.globalLocal[globalTVarId];
        },

        recordTVarVersion: function (localTVarId, version) {
            return this.localGlobal[localTVarId].version = version;
        },

        localTVarVersion: function (localTVarId) {
            return this.localGlobal[localTVarId].version;
        },

        create: function (txnLog) {
            var keys = Object.keys(txnLog.created),
                i, localTVar, globalTVar;

            for (i = 0; i < keys.length; i += 1) {
                localTVar = txnLog.created[keys[i]];
                globalTVar = TVar.create(localTVar.isArray, localTVar.value, this);
                this.addToMapping(globalTVar.id, keys[i]);
            }
        },

        checkThing: function (thing, updates) {
            var keys = Object.keys(thing),
                ok = true,
                i, localTVar, globalTVar;

            for (i = 0; i < keys.length; i += 1) {
                localTVar = thing[keys[i]];
                globalTVar = this.toGlobalTVar(keys[i]);
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

        bumpCreated: function (txnLog) {
            // Regardless of success of commit or retry, we always
            // grab creates.  If we read from, or wrote to any of the
            // objs we created, we will record in the txnlog that we
            // read from or wrote to version 0 of such objects. Thus
            // we delay the bumping to version 1 until after the
            // read/write checks, and we then update ourself to
            // remember we've seen version 1.
            var keys = Object.keys(txnLog.created),
                i, globalTVar;
            for (i = 0; i < keys.length; i += 1) {
                globalTVar = this.toGlobalTVar(keys[i]);
                globalTVar.bump();
                this.recordTVarVersion(keys[i], globalTVar.version);
            }
        },

        retry: function (txnLog) {
            var txnId = txnLog.txnId,
                updates = {},
                ok, self, fired, observer, keys, i, localTVar, globalTVar;

            this.create(txnLog);
            ok = this.checkReads(txnLog, updates);

            this.bumpCreated(txnLog);

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
                                self.sendUpdates(updates);
                                self.write({type: "retry", txnId: txnId});
                            }
                        });
                };

                keys = Object.keys(txnLog.read);
                for (i = 0; i < keys.length; i += 1) {
                    (this.toGlobalTVar(keys[i])).subscribe(observer);
                }
            } else {
                // oh good, need to do updates already and can then
                // immediately restart the txn.
                this.sendUpdates(updates);
                this.write({type: "retry", txnId: txnId});
            }

        },

        commit: function (txnLog) {
            var txnId = txnLog.txnId,
                updates = {},
                ok, keys, i, j, localTVar, globalTVar, future, names, name, value;

            this.create(txnLog);
            // Prevent a short-cut impl of && from causing us problems
            ok = this.checkReads(txnLog, updates);
            ok = this.checkWrites(txnLog, updates) && ok;

            this.bumpCreated(txnLog);

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
                    globalTVar = this.toGlobalTVar(keys[i]);
                    names = Object.keys(localTVar.children);
                    for (j = 0; j < names.length; j += 1) {
                        name = names[j];
                        value = localTVar.children[name];
                        if (util.hasOwnProp.call(value, 'tvar')) {
                            value.value = {tvar: this.toGlobalTVarID(value.tvar)};
                            delete value.tvar;
                            Object.defineProperty(globalTVar.raw, name, value);
                        } else if (util.hasOwnProp.call(value, 'deleted')) {
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
                    // wait until after the bump before recording new version
                    this.recordTVarVersion(keys[i], globalTVar.version);
                }

                future.force();
            } else {
                this.sendUpdates(updates);
                this.write({type: "commit", txnId: txnId, result: "failure"});
            }
        },

        onPreCommit: function (txnLog) {
            return true;
        },

        sendUpdates: function (updates) {
            var empty = true,
                keys = Object.keys(updates),
                txnLog = {type: "updates", updates: {}},
                key, globalTVar, localTVar, names, name, i, desc, j;

            while (0 < keys.length) {
                key = keys.shift();
                globalTVar = globalTVars[key];
                localTVar = {version: globalTVar.version,
                             value:   {},
                             isArray: globalTVar.isArray};

                i = this.toLocalTVarID(globalTVar.id);
                if (undefined === i) {
                    txnLog.updates[this.addToMapping(globalTVar.id)] = localTVar;
                    empty = false;
                } else {
                    if (this.localTVarVersion(i) === globalTVar.version) {
                        // Some earlier update caused us to send the
                        // new version of this TVar down to the
                        // client. Thus skip it here.
                        continue;
                    } else {
                        txnLog.updates[i] = localTVar;
                        empty = false;
                    }
                }
                this.recordTVarVersion(i, globalTVar.version);

                names = Object.getOwnPropertyNames(globalTVar.raw);
                for (i = 0; i < names.length; i += 1) {
                    name = names[i];
                    desc = Object.getOwnPropertyDescriptor(globalTVar.raw, name);
                    if (util.hasOwnProp.call(desc.value, 'tvar')) {
                        localTVar.value[name] = desc;
                        j = this.toLocalTVarID(desc.value.tvar);
                        if (undefined === j) {
                            keys.push(desc.value.tvar); // push the global id
                            desc.tvar = this.addToMapping(desc.value.tvar);
                        } else {
                            desc.tvar = j;
                        }
                        delete desc.value;
                    } else {
                        localTVar.value[name] = desc;
                    }
                }
            }

            if (! empty) {
                this.write(txnLog);
            }
            return ! empty;
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
