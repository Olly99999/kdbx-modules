/ heartbeat module for kdb-x
/ every process can publish a periodic heartbeat over pub/sub so that downstream
/ monitors can detect when a process has stopped beating - i.e. it is stalled or
/ blocked - even when the underlying connection is still valid
/ the module handles both publishing heartbeats and, on the monitoring side,
/ storing received heartbeats and raising warnings / errors when they stop
/ config and dependencies are passed to init in a single dictionary: config keys (see heartbeat.md)
/ are optional with defaults; log/timer/pubsub are required (servers/handlers when monitoring) and
/ init errors immediately if a required dependency is missing - see heartbeat.md
/ module-local state convention: read config bare, mutate via .z.m, and access
/ injected dependencies via .z.m at every call site

/ ============================================================
/ module state and defaults
/ ============================================================

/ table used to publish heartbeats - sym holds the publishing process type
heartbeat:([] time:`timestamp$(); sym:`symbol$(); procname:`symbol$(); counter:`long$(); pid:`int$(); host:`symbol$(); port:`int$());

/ keyed store of the latest received heartbeat per process, with warning / error state
hb:update warning:0b,error:0b from `sym`procname xkey heartbeat;

/ remote handles we have already subscribed to for heartbeats
subscribedhandles:`int$();

/ heartbeat counter - bumped on each publish
hbcounter:0;

/ current-time function - heartbeat owns its clock; override via setcp for testing / simulation
cp:{.z.p};

/ configuration defaults - overridden by the config dictionary passed to init
enabled:1b;                   / whether heartbeat publishing / checking is enabled
subenabled:0b;                / whether this process monitors (subscribes to) other heartbeats
debug:1b;                     / whether to log warning / error transitions
publishinterval:0D00:00:30;   / how often heartbeats are published
checkinterval:0D00:00:10;     / how often received heartbeats are checked
warningtolerance:1.5;         / warning after warningtolerance*publishinterval without a beat
errortolerance:2f;            / error after errortolerance*publishinterval without a beat
proctype:`unknown;            / this process's type (published as sym)
procname:.z.h;                / this process's name
pid:.z.i;                     / this process's pid
host:.z.h;                    / this process's host
port:`int$system"p";          / this process's port
connections:`$();             / process types this monitor should subscribe to
onwarning:{[procs]};          / callback fired with the rows entering warning state
onerror:{[procs]};            / callback fired with the rows entering error state

/ ============================================================
/ internal helpers
/ ============================================================

normlog:{[logdict]
  / detect kx.log instance by presence of kx.log-specific keys (getlvl, sinks, fmts)
  / kx.log functions are monadic - wrap each into binary {[c;m]} and embed context in the message
  / plain {[c;m]} log dicts (info`warn`error only) pass through unchanged
  $[any `getlvl`sinks`fmts in key logdict;
    `info`warn`error!(
      {[fn;c;m] fn[string[c],": ",m]}[logdict`info;];
      {[fn;c;m] fn[string[c],": ",m]}[logdict`warn;];
      {[fn;c;m] fn[string[c],": ",m]}[logdict`error;]);
    logdict]
  };

/ extract a required dependency dict, erroring if absent, null, not a dict, or missing a required key
/ nested if guards (not and) - key would throw on a non-dict if evaluated eagerly
requiredep:{[deps;name;reqkeys]
  d:$[99h=type deps;$[(name in key deps) and not (::)~deps name;deps name;()!()];()!()];
  if[not count d;
    '"di.heartbeat: ",(string name)," dependency is required and must be a non-empty function dict; pass it via init deps - see heartbeat.md"];
  if[99h<>type d;
    '"di.heartbeat: ",(string name)," dependency must be a dict of functions"];
  if[not all reqkeys in key d;
    '"di.heartbeat: ",(string name)," dependency must provide ",(", " sv string reqkeys),"; got: ",", " sv string key d];
  d
  };

/ warning / error grace periods - vary by process type if required
warningperiod:{[processtype] `timespan$warningtolerance*publishinterval};
errorperiod:{[processtype] `timespan$errortolerance*publishinterval};

/ convert a timespan into whole seconds for the timer period
tosecs:{[span] `int$span%0D00:00:01};

/ wire the injected dependencies from the single deps dict (which also carries config keys)
setdeps:{[deps]
  / validate every dependency before writing any module state so a failed init leaves state untouched
  / log, timer and pubsub are required; servers and handlers only when monitoring
  / nested if guards (not and) - and evaluates both sides eagerly and key would throw on a non-dict
  if[99h<>type deps;
    '"di.heartbeat: deps must be a dictionary of config and injected dependencies - see heartbeat.md"];
  / log - required: info/warn/error (heartbeat uses all three); a kx.log instance is auto-wrapped
  / to the binary {[c;m]} contract by normlog, so it can be passed directly
  if[not `log in key deps;
    '"di.heartbeat: log dependency is required; pass `info`warn`error (or a kx.log logger) keyed on `log"];
  if[99h<>type deps`log;
    '"di.heartbeat: log must be a dict of info/warn/error functions (or a kx.log logger)"];
  lg:normlog deps`log;
  if[not all `info`warn`error in key lg;
    '"di.heartbeat: log must provide info/warn/error; got: ",", " sv string key lg];
  timerdict:requiredep[deps;`timer;`addjob`deletejobs];
  pubsubdict:requiredep[deps;`pubsub;`publish`subscribe];
  / servers/handlers required only when monitoring; take subenabled from deps as setconfig runs after
  sub:$[`subenabled in key deps;deps`subenabled;0b];
  serversdict:$[sub;requiredep[deps;`servers;enlist`getservers];()!()];
  handlersdict:$[sub;requiredep[deps;`handlers;enlist`register];()!()];
  / all validated - wire module state (no further throws)
  .z.m.log:lg;
  .z.m.timeraddjob:timerdict`addjob;
  .z.m.timerdeletejobs:timerdict`deletejobs;
  .z.m.pubsubpublish:pubsubdict`publish;
  .z.m.pubsubsubscribe:pubsubdict`subscribe;
  if[sub;setmonitordeps[serversdict;handlersdict]];
  };

/ wire the monitor-only dependencies, required only when subenabled (this process monitors others)
setmonitordeps:{[serversdict;handlersdict]
  / wire the monitor-only dependencies (already validated by setdeps) - split out so the
  / subenabled conditional in setdeps stays a single statement per the coding standards
  .z.m.serversgetservers:serversdict`getservers;
  .z.m.handlersregister:handlersdict`register;
  };

/ apply recognised config overrides from the deps dict, defaulting where a key is absent
setconfig:{[deps]
  / explicit per-key .z.m assignment (like di.eodtime) - no .z.M, and reinit resets to defaults
  cfg:$[99h=type deps;deps;()!()];
  .z.m.enabled:$[`enabled in key cfg;cfg`enabled;1b];
  .z.m.subenabled:$[`subenabled in key cfg;cfg`subenabled;0b];
  .z.m.debug:$[`debug in key cfg;cfg`debug;1b];
  .z.m.publishinterval:$[`publishinterval in key cfg;cfg`publishinterval;0D00:00:30];
  .z.m.checkinterval:$[`checkinterval in key cfg;cfg`checkinterval;0D00:00:10];
  .z.m.warningtolerance:$[`warningtolerance in key cfg;cfg`warningtolerance;1.5];
  .z.m.errortolerance:$[`errortolerance in key cfg;cfg`errortolerance;2f];
  .z.m.proctype:$[`proctype in key cfg;cfg`proctype;`unknown];
  .z.m.procname:$[`procname in key cfg;cfg`procname;.z.h];
  .z.m.pid:$[`pid in key cfg;cfg`pid;.z.i];
  .z.m.host:$[`host in key cfg;cfg`host;.z.h];
  .z.m.port:$[`port in key cfg;cfg`port;`int$system"p"];
  .z.m.connections:$[`connections in key cfg;cfg`connections;`$()];
  .z.m.onwarning:$[`onwarning in key cfg;cfg`onwarning;{[procs]}];
  .z.m.onerror:$[`onerror in key cfg;cfg`onerror;{[procs]}];
  };

/ schedule the periodic heartbeat jobs via the injected timer
registertimers:{
  / mode 2 = period after previous actual start - a heartbeat says "alive now", so
  / missed beats must not be replayed as a catch-up storm (which mode 1 would do)
  / clear any previously-registered jobs first so init is safe to call again
  .z.m.timerdeletejobs[`hbpublish`hbcheck`hbsubscribe];
  if[enabled;
    .z.m.timeraddjob[`hbpublish;publishheartbeat;();tosecs publishinterval;2;()!()];
    .z.m.timeraddjob[`hbcheck;checkheartbeat;();tosecs checkinterval;2;()!()]];
  if[subenabled;
    .z.m.timeraddjob[`hbsubscribe;hbsubscriptions;();60;2;()!()]];
  };

/ wire the connection-close cleanup through the injected handler manager
registerhandlers:{
  if[subenabled;
    .z.m.handlersregister[`.z.pc;`heartbeat;closeconnection]];
  };

/ log a single process moving into warning state
logwarnproc:{[r]
  .z.m.log[`warn][`heartbeat;"process ",(string r`procname)," (type ",(string r`sym),") has not heartbeated since ",string r`time];
  };

/ log a single process moving into error state
logerrproc:{[r]
  .z.m.log[`error][`heartbeat;"process ",(string r`procname)," (type ",(string r`sym),") has not heartbeated since ",string r`time];
  };

/ move processes into warning state, log and fire the warning callback
/ project error:0b too so a warning transition fully defines the flags (a status=1 row is below the error threshold)
warn:{[procs]
  if[debug;logwarnproc each 0!procs];
  .z.m.hb:hb upsert select sym,procname,warning:1b,error:0b from procs;
  onwarning procs;
  };

/ move processes into error state, log and fire the error callback
/ project warning:0b too so an error transition fully defines the flags (mutually exclusive with warning)
err:{[procs]
  if[debug;logerrproc each 0!procs];
  .z.m.hb:hb upsert select sym,procname,warning:0b,error:1b from procs;
  onerror procs;
  };

/ subscribe to a single remote heartbeat publisher, logging and skipping on failure
subscribeone:{[h]
  ok:@[{.z.m.pubsubsubscribe x;1b};h;{[hdl;e] .z.m.log[`error][`heartbeat;"failed to subscribe to heartbeats on handle ",(string hdl),": ",e];0b}[h]];
  if[ok;.z.m.subscribedhandles:distinct subscribedhandles,h];
  };

/ subscribe to publishers of the given process type(s) that are not yet subscribed
getheartbeats:{[proctype]
  handles:(.z.m.serversgetservers proctype) except subscribedhandles;
  if[count handles;
    .z.m.log[`info][`heartbeat;"subscribing to new heartbeat handle(s) ",", " sv string handles];
    subscribe handles];
  };

/ subscribe to all configured heartbeat publishers (by configured process type) - timer job
hbsubscriptions:{
  getheartbeats connections;
  };

/ drop a closed handle from the tracked subscriptions - registered against .z.pc
closeconnection:{[h]
  .z.m.subscribedhandles:subscribedhandles except h;
  };

/ ============================================================
/ public api
/ ============================================================

/ publish a single heartbeat row over pub/sub and bump the counter
publishheartbeat:{
  if[not enabled;:()];
  .z.m.pubsubpublish[`heartbeat;enlist `time`sym`procname`counter`pid`host`port!(cp[];proctype;procname;hbcounter;pid;host;port)];
  .z.m.hbcounter:hbcounter+1;
  };

/ flag processes that have not heartbeated within the warning / error grace periods - timer job
checkheartbeat:{
  / status: 0 healthy, 1 warning, 2+ error
  / grace periods are computed as locals first - module functions do not resolve inside qsql
  now:cp[];
  t:0!hb;
  wp:warningperiod each t`sym;
  ep:errorperiod each t`sym;
  stats:update status:(`short$now>time+wp)+`short$2*now>time+ep from t;
  / newwarn/newerr are rows of the store (built from 0!hb), so warn/err only update flags on
  / existing rows and never insert - avoids seeding a null-time row that would look instantly stale
  newwarn:select sym,procname,time from stats where status=1,not warning;
  newerr:select sym,procname,time from stats where status>1,not error;
  if[count newwarn;warn newwarn];
  if[count newerr;err newerr];
  };

/ store one or more incoming heartbeats, keeping the latest per process and clearing warning / error state
storeheartbeat:{[batch]
  / call this from upd when a heartbeat arrives
  .z.m.hb:hb upsert update warning:0b,error:0b from select by sym,procname from batch;
  };

/ seed the store with expected processes so a never-seen process is flagged
addprocs:{[proctypes;procnames]
  / real heartbeats arriving later override these seeded rows
  seed:2!([]sym:proctypes,();procname:procnames,();time:cp[];counter:0N;pid:0Ni;host:`;port:0Ni;warning:0b;error:0b);
  .z.m.hb:seed,hb;
  };

/ subscribe to heartbeats on the given remote handle(s), tracking successful subscriptions
subscribe:{[handles]
  subscribeone each (),handles;
  };

/ return the current heartbeat store for inspection
gethb:{hb};

/ replace the current-time function (used by tests and simulation)
setcp:{[f] .z.m.cp:f};

init:{[deps]
  / initialise from a single dictionary holding config overrides and injected dependencies - see heartbeat.md
  / config keys (see heartbeat.md) are optional and fall back to defaults; dependencies are required:
  /   `log      - a logger - required; must provide info/warn/error (a kx.log instance is auto-wrapped)
  /   `timer    - `addjob`deletejobs (full di.timer dict may be passed) - required
  /   `pubsub   - `publish`subscribe          - required
  /   `servers  - `getservers                 - required when subenabled
  /   `handlers - `register`remove`list       - required when subenabled
  / note: the module keeps its own clock (cp, default .z.p) - override via setcp
  / example:
  /   heartbeat.init[`proctype`procname`log`timer`pubsub!(`rdb;`rdb1;kxlog;timerdep;psdep)]
  setdeps deps;
  setconfig deps;
  registertimers[];
  registerhandlers[];
  .z.m.log[`info][`heartbeat;"di.heartbeat initialised"];
  };
