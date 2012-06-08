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

            liftFunctions: function (src, dest, fields) {
                var i, field;
                for (i = 0; i < fields.length; i += 1) {
                    field = fields[i];
                    dest[field] = src[field].bind(src);
                }
            }
        };
    }());

    function TVar(id, isArray, raw) {
        this.raw = isArray ? [] : {};
        util.shallowCopy(raw, this.raw);
        this.isArray = isArray;
        this.id = id;
        this.version = 0;
        this.observers = [];
        this.lastTxnSet = {dependencies: this.emptyDependencies};
        this.firstTxnSet = this.lastTxnSet;
        this.txnSetLength = 1;
    }
    TVar.prototype = {
        emptyDependencies: {},

        txnSetLengthLimit: 64, // MAGIC NUMBER!

        resolve: function (fieldName) {
            if (util.hasOwnProp.call(this.raw, fieldName)) {
                if (util.hasOwnProp.call(this.raw[fieldName], 'tvar')) {
                    return globalTVars[this.raw[fieldName].tvar];
                } else {
                    return this.raw[fieldName];
                }
            } else {
                return undefined;
            }
        },

        subscribe: function (fun) {
            this.observers.push(fun);
        },

        bump: function (future) {
            // Create a fresh observers array first, otherwise if
            // firing the observer creates a new subscription, you can
            // get into a loop!
            var observers = this.observers,
                i;
            this.version += 1;
            this.observers = [];
            for (i = 0; i < observers.length; i += 1) {
                (observers[i])(this, future);
            }
        },

        appendDependencies: function (dependencies) {
            var txnSet = { dependencies: dependencies };
            this.lastTxnSet.next = txnSet;
            this.lastTxnSet = txnSet;
            this.txnSetLength += 1;
        },

        rollUpTxnSets: function () {
            var txnSet = this.firstTxnSet,
                dependencies = {},
                ids, i, id, nextTxnSet;
            if (this.txnSetLength > this.txnSetLengthLimit) {
                // We're going to roll up everything, and then pop it
                // into the current lastTxnSet dependencies
                // field. This means that clients which are already
                // pointing there will not have to repeat work.  Note
                // that because lastTxnSet.dependencies will be shared
                // across several TVars, we must create a new
                // lastTxnSet.dependencies which is non-shared, and
                // then only rewrite our own lastTxnSet.dependencies
                // field.

                while (undefined !== txnSet) {
                    ids = Object.keys(txnSet.dependencies);
                    for (i = 0; i < ids.length; i += 1) {
                        id = ids[i];
                        dependencies[id] = txnSet.dependencies[id];
                    }
                    // Wipe out the old dependencies to avoid work
                    // later on, and rewrite pointers to enable faster
                    // catchup.
                    txnSet.dependencies = this.emptyDependencies;
                    nextTxnSet = txnSet.next;
                    txnSet.next = this.lastTxnSet;
                    txnSet = nextTxnSet;
                }
                delete this.lastTxnSet.next; // remove the loop we just created!
                this.lastTxnSet.dependencies = dependencies;
                this.firstTxnSet = this.lastTxnSet;
                this.txnSetLength = 1;
            }
        }
    };

    TVar.create = function (isArray, value, securityProvider) {
        var globalTVar;
        globalTVarCount += 1;
        globalTVar = new TVar(globalTVarCount, isArray, value);
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
        util.liftFunctions(
            this.emitter, this,
            ['on', 'once', 'removeListener', 'removeAllListeners', 'emit']);

        this.securityProvider = serverEventEmitter.securityProvider;
        this.connection = connection;
        this.id = connection.id;
        this.minTVarId = 0;

        this.localGlobal = {};
        this.globalLocal = {};
        globalLocal[connection.id] = this.globalLocal;

        this.addToMapping(rootTVar.id, rootTVar.id);

        connection.on('data', this.data.bind(this));
        connection.on('close', this.close.bind(this));
        serverEventEmitter.emit('connection', this);
    }

    Client.prototype = {
        isAuthenticated: false,

        data: function (message) {
            try {
                if (this.isAuthenticated) {
                    this.dispatch(message);
                } else {
                    this.emit('data', message, this);
                }
            } catch (err) {
                this.connection.close(500, "" + err);
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
                this.log("Received unexpected message from client:");
                this.log(txnLog);
            }
        },

        log: function (msg) {
            console.log(this.connection.id + ": " + msg);
        },

        addToMapping: function (globalTVarId, localTVarId) {
            if (undefined === localTVarId) {
                this.minTVarId -= 1;
                localTVarId = this.minTVarId;
            }
            this.localGlobal[localTVarId] = {id: globalTVarId,
                                             version: 0,
                                             txnSet: undefined};
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

        recordTVarVersion: function (localTVarId, globalTVar) {
            var entry = this.localGlobal[localTVarId];
            entry.version = globalTVar.version;
            entry.txnSet = globalTVar.lastTxnSet;
            return entry;
        },

        recordTVarUnpopulated: function (localTVarId, globalTVar) {
            var entry = this.localGlobal[localTVarId];
            entry.version = 0;
            entry.txnSet = globalTVar.lastTxnSet;
            return entry;
        },

        localTVarVersion: function (localTVarId) {
            return this.localGlobal[localTVarId].version;
        },

        localTVarTxnSet: function (localTVarId) {
            // we've already done txnSet. Hence .next.
            var txnSet = this.localGlobal[localTVarId].txnSet;
            return undefined === txnSet ? txnSet : txnSet.next;
        },

        create: function (txnLog) {
            var ids = Object.keys(txnLog.created),
                ok = true,
                i, localTVar, globalTVar;

            for (i = 0; i < ids.length; i += 1) {
                localTVar = txnLog.created[ids[i]];
                globalTVar = TVar.create(localTVar.isArray, localTVar.value);
                if (this.securityProvider.lifted(this, globalTVar, localTVar.meta)) {
                    this.addToMapping(globalTVar.id, ids[i]);
                } else {
                    ok = false;
                }
            }
            return ok;
        },

        checkThing: function (thing, updates, action) {
            var ids = Object.keys(thing),
                ok = true,
                i, localTVar, globalTVar;

            // Do not short circuit this! We want to send as many
            // updates as possible to the client.
            for (i = 0; i < ids.length; i += 1) {
                localTVar = thing[ids[i]];
                globalTVar = this.toGlobalTVar(ids[i]);
                if (this.securityProvider[action](this, globalTVar, localTVar)) {
                    if (localTVar.version !== globalTVar.version) {
                        updates[globalTVar.id] = true;
                        ok = false;
                    }
                } else {
                    ok = false;
                }
            }

            return ok;
        },

        checkReads: function (txnLog, updates) {
            return this.checkThing(txnLog.read, updates, 'read');
        },

        checkWrites: function (txnLog, updates) {
            return this.checkThing(txnLog.written, updates, 'written');
        },

        bumpCreated: function (txnLog, dependencies) {
            // Regardless of success of commit or retry, we always
            // grab creates.  If we read from, or wrote to any of the
            // objs we created, we will record in the txnlog that we
            // read from or wrote to version 0 of such objects. Thus
            // we delay the bumping to version 1 until after the
            // read/write checks, and we then update ourself to
            // remember we've seen version 1.
            var ids = Object.keys(txnLog.created),
                i, globalTVar;
            for (i = 0; i < ids.length; i += 1) {
                globalTVar = this.toGlobalTVar(ids[i]);
                globalTVar.bump();
                globalTVar.appendDependencies(dependencies);
                dependencies[globalTVar.id] = globalTVar.version;
                this.recordTVarVersion(ids[i], globalTVar);
            }
        },

        retry: function (txnLog) {
            var txnId = txnLog.txnId,
                updates = {},
                dependencies = {},
                ok, self, fired, observer, ids, i, localTVar, globalTVar;

            ok = this.create(txnLog);
            ok = this.checkReads(txnLog, updates) && ok;

            this.bumpCreated(txnLog, dependencies);

            if (ok) {
                // -fired is there to make sure we only send the
                // updates to this client once
                // - updates is there to collect all the updates
                // relevant for this txn together
                // - future is there to make sure we only do the
                // sending once the corresponding commit is fully done
                // and have thus built the biggest update we can
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
                                self.write({type: 'result', txnId: txnId, result: 'restart'});
                            }
                        });
                };

                ids = Object.keys(txnLog.read);
                for (i = 0; i < ids.length; i += 1) {
                    (this.toGlobalTVar(ids[i])).subscribe(observer);
                }
            } else {
                // oh good, need to do updates already and can then
                // immediately restart the txn.
                this.sendUpdates(updates);
                this.write({type: 'result', txnId: txnId, result: 'restart'});
            }
        },

        commit: function (txnLog) {
            var txnId = txnLog.txnId,
                updates = {},
                dependencies = {},
                ok, future, globalIds, localIds, i, j, localTVar, globalTVar, names, name, desc;

            // Prevent a short-cut impl of && from causing us problems
            ok = this.create(txnLog);
            ok = this.checkReads(txnLog, updates) && ok;
            ok = this.checkWrites(txnLog, updates) && ok;

            this.bumpCreated(txnLog, dependencies);

            if (ok && this.onPreCommit(txnLog)) {
                // send out the success message first. This a)
                // improves performance; and more importantly b)
                // ensures that any updates that end up being sent to
                // the same client (due to pending retries) get
                // received after the success msg and thus we don't
                // fall out of step with vsns
                this.write({type: 'result', txnId: txnId, result: 'success'});

                future = new CoalescingObserver();

                // Once we've committed, and all tvars in this txn
                // have appended the new dependencies and been bumped,
                // only then consider rolling up.
                future.insert(dependencies, function () {
                    globalIds = Object.keys(dependencies);
                    for (i = 0; i < globalIds.length; i += 1) {
                        globalTVars[globalIds[i]].rollUpTxnSets();
                    }
                });

                localIds = Object.keys(txnLog.written);
                for (i = 0; i < localIds.length; i += 1) {
                    localTVar = txnLog.written[localIds[i]];
                    globalTVar = this.toGlobalTVar(localIds[i]);
                    names = Object.keys(localTVar.children);
                    for (j = 0; j < names.length; j += 1) {
                        name = names[j];
                        desc = localTVar.children[name];
                        if (util.hasOwnProp.call(desc, 'tvar')) {
                            desc.value = {tvar: this.toGlobalTVarID(desc.tvar)};
                            delete desc.tvar;
                            Object.defineProperty(globalTVar.raw, name, desc);
                        } else if (util.hasOwnProp.call(desc, 'deleted')) {
                            delete globalTVar.raw[name];
                        } else {
                            // mess for dealing with arrays, and some proxy stuff
                            if (globalTVar.isArray && 'length' === name) {
                                globalTVar.raw[name] = desc.value;
                            } else {
                                Object.defineProperty(globalTVar.raw, name, desc);
                            }
                        }
                    }
                    globalTVar.bump(future);
                    // wait until after the bump before recording new version
                    globalTVar.appendDependencies(dependencies);
                    dependencies[globalTVar.id] = globalTVar.version;
                    this.recordTVarVersion(localIds[i], globalTVar);
                }

                future.force();
            } else {
                this.sendUpdates(updates);
                this.write({type: 'result', txnId: txnId, result: 'failure'});
            }
        },

        onPreCommit: function (txnLog) {
            return true;
        },

        sendUpdates: function (updates) {
            var globalIds = Object.keys(updates),
                unpopulateds = {},
                txnLog = {type: "updates", updates: {}},
                globalId, globalTVar, localId, localTVar, txnSet,
                globalIds1, globalId1, localId1, names, name, i, desc;

            while (0 < globalIds.length) {
                globalId = globalIds.shift();
                globalTVar = globalTVars[globalId];
                localTVar = {version: globalTVar.version,
                             value:   {},
                             isArray: globalTVar.isArray};

                localId = this.toLocalTVarID(globalTVar.id);
                if (undefined === localId) {
                    localId = this.addToMapping(globalTVar.id);
                    txnLog.updates[localId] = localTVar;
                } else {
                    if (this.localTVarVersion(localId) === globalTVar.version) {
                        // Some earlier update caused us to send the
                        // new version of this TVar down to the
                        // client. Thus skip it here.
                        continue;
                    } else {
                        txnLog.updates[localId] = localTVar;
                    }
                }

                // Expand based on the dependencies of the globalTVar's txnSet chain
                txnSet = this.localTVarTxnSet(localId);
                if (undefined === txnSet) {
                    txnSet = globalTVar.firstTxnSet;
                }
                while (undefined !== txnSet) {
                    globalIds1 = Object.keys(txnSet.dependencies);
                    for (i = 0; i < globalIds1.length; i += 1) {
                        globalId1 = globalIds1[i];
                        if (! util.hasOwnProp.call(updates, globalId1)) {
                            localId1 = this.toLocalTVarID(globalId1);
                            if (undefined !== localId1 &&
                                this.localTVarVersion(localId1) < txnSet.dependencies[globalId1]) {
                                globalIds.push(globalId1);
                                updates[globalId1] = true;
                            }
                        }
                    }
                    txnSet = txnSet.next;
                }

                if (util.hasOwnProp.call(unpopulateds, globalId)) {
                    localTVar.version = 0;
                    this.recordTVarUnpopulated(localId, globalTVar);
                } else {
                    this.recordTVarVersion(localId, globalTVar);

                    // Expand based on the object's fields
                    names = Object.getOwnPropertyNames(globalTVar.raw);
                    names = this.securityProvider.filterUpdateNames(this, globalTVar, names);
                    for (i = 0; i < names.length; i += 1) {
                        name = names[i];
                        desc = Object.getOwnPropertyDescriptor(globalTVar.raw, name);
                        desc = this.securityProvider.filterUpdateField(this, globalTVar, name, desc);
                        if (util.hasOwnProp.call(desc.value, 'tvar')) {
                            localTVar.value[name] = desc;
                            localId1 = this.toLocalTVarID(desc.value.tvar);
                            if (undefined === localId1) {
                                if (! util.hasOwnProp.call(updates, desc.value.tvar)) {
                                    unpopulateds[desc.value.tvar] = true;
                                    globalIds.push(desc.value.tvar);
                                }
                                desc.tvar = this.addToMapping(desc.value.tvar);
                            } else {
                                desc.tvar = localId1;
                            }
                            // we are sending this tvar. So we can happily
                            // ensure that no other txnSet can ask us to
                            // send it
                            updates[desc.value.tvar] = true;
                            delete desc.value;
                        } else {
                            localTVar.value[name] = desc;
                        }
                    }
                }
            }

            if (0 < Object.keys(txnLog.updates).length) {
                this.write(txnLog);
                return true;
            }
            return false;
        }
    };

    function ServerEventEmitter (securityProvider) {
        this.securityProvider = securityProvider;
        this.emitter = new events.EventEmitter();
        util.liftFunctions(
            this.emitter, this,
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

    function DefaultSecurityProvider () {
    }
    DefaultSecurityProvider.prototype = {
        lifted: function (client, globalTVar, meta) {
            return true;
        },

        read: function (client, globalTVar, localTVar) {
            return true;
        },

        written: function (client, globalTVar, localTVar) {
            return true;
        },

        filterUpdateNames: function (client, globalTVar, fieldNames) {
            return fieldNames;
        },

        filterUpdateField: function (client, globalTVar, fieldName, desc) {
            return desc;
        }
    };

    create = function (http, path, securityProvider, root) {
        var server = sockjs.createServer(sockjs_opts),
            emitter;

        if (undefined === path || null === path) {
            path = '[/]atomize';
        }
        if (undefined === securityProvider || null === securityProvider) {
            securityProvider = new DefaultSecurityProvider();
        }
        emitter = new ServerEventEmitter(securityProvider);

        server.on('connection', emitter.connection.bind(emitter));
        http.addListener('upgrade', function (req, res) { res.end(); });
        server.installHandlers(http, {prefix: path});

        if (undefined === root || null === root || util.isPrimitive(root)) {
            root = {};
        }
        globalTVarCount = 1;
        rootTVar = new TVar(globalTVarCount, Array.isArray(root), root);
        globalTVars[rootTVar.id] = rootTVar;
        rootTVar.bump();

        emitter.client = function () { return new atomize.Atomize(Client, emitter); };

        return emitter;
    };

    exports.create = create;
}());
