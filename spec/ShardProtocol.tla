--------------------------- MODULE ShardProtocol ---------------------------
(***************************************************************************)
(* The SkyS3 shard replication protocol (design sections 5 and 6).        *)
(*                                                                         *)
(* One shard, replicated on a small set of nodes. The model covers:       *)
(*                                                                         *)
(*   - the all-member commit rule (section 5.1),                           *)
(*   - configuration changes by compare-and-swap on the shard register,   *)
(*     under rules R1 to R3 (section 6.3),                                 *)
(*   - member removal (section 6.4),                                       *)
(*   - primary takeover after primary_grace, and reconciliation with       *)
(*     TRUNCATE and roll-forward (sections 6.5 and 6.6),                   *)
(*   - learners: live stream first, backfill, promotion without pausing   *)
(*     commits, and re-admission of a node with an old log (6.4, 6.7),    *)
(*   - planned handoff by step-down (section 5.4),                         *)
(*   - leases granted by every member, on clocks whose rates drift within *)
(*     a bound (section 5.4),                                              *)
(*   - node restarts, which keep durable state and lose volatile state,   *)
(*   - gateways reading with stale shard maps (see Read).                  *)
(*                                                                         *)
(* It checks the three properties of section 6.8: committed records       *)
(* survive, one committing primary per epoch, and linearizable reads.     *)
(*                                                                         *)
(* Seeded bugs. The constant Bug is "none" or names one deliberate        *)
(* protocol error from BugNames. Each removes one guard the design relies *)
(* on. CI checks that the model checker finds a violation for every bug,  *)
(* so the model cannot silently stop exercising that guard.               *)
(*                                                                         *)
(* Time. Real time advances in discrete ticks (action Tick). In each tick *)
(* every node's monotonic clock advances by some amount in Rates, chosen  *)
(* independently per node and per tick, so every clock runs at a rate     *)
(* between lo = Min(Rates) and hi = Max(Rates) local units per tick. That *)
(* is the drift bound rho of section 2.3 around the nominal rate          *)
(* (lo + hi) / 2, with (1 + rho) / (1 - rho) = hi / lo. Timers hold the   *)
(* time remaining in the owning node's local units, which keeps the state *)
(* space finite without an absolute clock. Every other action takes no    *)
(* time.                                                                   *)
(*                                                                         *)
(* A lease round (beacon, acknowledgement, grant) is one action. In the  *)
(* protocol the primary's lease counts from when it sent the beacon and   *)
(* the member's grace from when it acknowledged it, so any delay only     *)
(* shortens the lease and postpones the grace: the atomic round is the    *)
(* worst case for reads. Taking the extreme rates in every tick is the    *)
(* worst case for the lease inequality of section 5.4,                    *)
(*   primary_grace >= primary_lease * (1 + rho) / (1 - rho),               *)
(* which with integer rates reads Grace * lo >= Lease * hi                *)
(* (GraceCoversDrift). The margin of section 5.4 covers the time between *)
(* a lease check and the index read it admits; here a read is atomic with *)
(* its lease check, so the model needs no margin.                          *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS
    Nodes,            \* nodes that can hold a replica of the shard
    InitPrimary,      \* the primary of the initial configuration
    InitMembers,      \* its members; the other nodes are spares
    MaxEpoch,         \* bound: the highest epoch the shard register reaches
    MaxWrites,        \* bound: the number of client writes
    MaxRestarts,      \* bound: the number of node restarts
    MinWriteReplicas, \* min_write_replicas (section 4.1)
    Lease,            \* primary_lease, in local clock units
    Grace,            \* primary_grace, in local clock units
    Rates,            \* local clock units a node's clock may advance per tick
    Bug               \* "none", or one of BugNames

BugNames == {
    \* R3: the primary promotes a learner that is in the acknowledgement set
    \* but has not finished backfill or is not durable up to the commit
    \* watermark.
    "promote_before_watermark",
    \* A member takes over, and so serves after reconciling, without waiting
    \* for primary_grace or a step-down.
    "serve_before_grace",
    \* R1: a candidate keeps granting leases after it has proposed itself.
    "candidate_keeps_granting",
    \* R2: a member accepts appends stamped with an epoch older than its own.
    "accept_older_epoch",
    \* The commit rule counts a majority of the members instead of all.
    "commit_on_majority",
    \* The register is written without comparing its version, so two
    \* configurations can share an epoch.
    "blind_register_write",
    \* Reconciliation truncates only the records past the new primary's last
    \* seq, and catch-up keeps a re-admitted learner's old records without
    \* checking their (epoch, seq) prefix.
    "truncate_by_seq_only",
    \* The primary stops waiting for a learner whose promotion CAS it has
    \* issued but whose outcome it does not know.
    "drop_promoting_learner",
    \* The primary serves reads during a promotion without a lease from the
    \* learner being promoted.
    "promotion_without_learner_lease",
    \* A restarted node takes over at once, forgetting that it may have
    \* granted the primary a lease just before it went down.
    "restart_forgets_grace",
    \* A restarted node forgets a takeover it proposed but has not seen the
    \* outcome of, and grants leases and acknowledges appends again.
    "restart_forgets_proposal",
    \* After a planned step-down, the old primary keeps serving reads to
    \* gateways with stale shard maps while its leases last.
    "stepped_down_keeps_reading",
    \* The step-down is volatile: a stepped-down primary that restarts
    \* serves again in the same epoch.
    "restart_forgets_step_down"}

ASSUME Bug \in BugNames \cup {"none"}
ASSUME InitMembers \subseteq Nodes /\ InitPrimary \in InitMembers
ASSUME MaxEpoch \in Nat \ {0} /\ MaxWrites \in Nat /\ MaxRestarts \in Nat
ASSUME MinWriteReplicas \in 1..Cardinality(Nodes)
ASSUME Rates \subseteq Nat \ {0} /\ Rates # {}
ASSUME Lease \in Nat \ {0} /\ Grace \in Nat

SetMin(S) == CHOOSE x \in S : \A y \in S : x <= y
SetMax(S) == CHOOSE x \in S : \A y \in S : x >= y
Max(a, b) == IF a >= b THEN a ELSE b
Min(a, b) == IF a <= b THEN a ELSE b

(* The lease inequality of section 5.4. A model that breaks it on purpose *)
(* says so in its configuration file.                                     *)
GraceCoversDrift == Grace * SetMin(Rates) >= Lease * SetMax(Rates)

Epochs == 1..MaxEpoch
Values == 1..MaxWrites

(* A log record: the epoch whose primary created it, and the client write *)
(* it carries. Its seq is its position in the log. A record keeps its     *)
(* creation epoch when a later primary rolls it forward, so (epoch, seq) *)
(* identifies it: one primary per epoch assigns each seq once.            *)
Record == [ep : Epochs, val : Values]

(* A slot a learner has not received yet: it joined the acknowledgement  *)
(* set before backfilling the records it lacks (section 6.4).             *)
Hole == [ep |-> 0, val |-> 0]

Config == [epoch : Epochs, primary : Nodes,
           members : SUBSET Nodes, learners : SUBSET Nodes]

InitConfig == [epoch |-> 1, primary |-> InitPrimary,
               members |-> InitMembers, learners |-> {}]

NoPromotion == [node |-> InitPrimary, epoch |-> 0]

(* Role states:                                                            *)
(*   backup       acknowledges appends and grants leases in its epoch     *)
(*   proposed     has stopped acknowledging and granting (R1) and issued  *)
(*                a takeover CAS whose outcome it does not know; durable  *)
(*   reconciling  primary of its configuration, not serving yet           *)
(*   serving      primary, accepting writes and serving reads             *)
(*   steppedDown  primary that has stepped down for a planned handoff;    *)
(*                durable                                                  *)
PStates == {"backup", "proposed", "reconciling", "serving", "steppedDown"}

VARIABLES
    reg,          \* the shard register in the control store
    conf,         \* conf[n]: n's latest durable CONFIG record
    log,          \* log[n]: n's log for the shard, after any TRUNCATE
    commit,       \* commit[p]: the commit watermark primary p knows
    pstate,       \* pstate[n]: role state, see PStates
    synced,       \* synced[p]: nodes whose log p has aligned with its own
    ackd,         \* ackd[p][m]: the seq up to which m has acknowledged to p
    ackSet,       \* ackSet[p]: learners p's commits wait for
    promoting,    \* promoting[p]: p's promotion CAS, until p knows the outcome
    leaseLeft,    \* leaseLeft[p][m]: p-local time left on m's lease to p
    graceLeft,    \* graceLeft[n]: n-local time until primary_grace has passed
                  \*   since n last granted a lease
    stepdowns,    \* step-down messages sent: [to, epoch]
    nextVal,      \* the next client write
    restarts,     \* restarts so far
    \* History variables, for the invariants only.
    committed,    \* <<seq, record>> pairs some primary has committed
    committers,   \* [epoch, node] pairs: node committed records in epoch
    observed,     \* the highest seq a client has seen, acknowledged or read
    readsOK       \* FALSE once a read returned state older than observed

regVars == <<reg>>
nodeVars == <<conf, log, commit, pstate>>
primaryVars == <<synced, ackd, ackSet, promoting>>
timeVars == <<leaseLeft, graceLeft>>
envVars == <<stepdowns, nextVal, restarts>>
historyVars == <<committed, committers, observed, readsOK>>
vars == <<regVars, nodeVars, primaryVars, timeVars, envVars, historyVars>>

-----------------------------------------------------------------------------
(* Helpers *)

ZeroMap == [m \in Nodes |-> 0]

IsPrimary(p) ==
    conf[p].primary = p /\ pstate[p] \in {"reconciling", "serving", "steppedDown"}

(* The learner whose promotion p has proposed and not seen the outcome   *)
(* of. The primary keeps waiting for its acknowledgements, and requires  *)
(* its lease for reads, until then.                                        *)
Promoting(p) == IF promoting[p].epoch > 0 THEN {promoting[p].node} ELSE {}

(* The nodes whose acknowledgements every commit at p waits for.         *)
AckGroup(p) ==
    (conf[p].members \cup ackSet[p]
        \cup (IF Bug = "drop_promoting_learner" THEN {} ELSE Promoting(p)))
    \ {p}

(* The nodes p needs a valid lease from to serve a read.                  *)
LeaseGroup(p) ==
    (conf[p].members
        \cup (IF Bug = "promotion_without_learner_lease" THEN {} ELSE Promoting(p)))
    \ {p}

(* A node acknowledges appends and grants leases only as a backup. A     *)
(* candidate stops both before it proposes (R1).                          *)
GrantsLeases(m) ==
    \/ pstate[m] = "backup"
    \/ Bug = "candidate_keeps_granting" /\ pstate[m] = "proposed"

(* R2: m accepts p's appends only in its own epoch. With the seeded bug  *)
(* it also accepts them from an older epoch.                              *)
EpochOK(p, m) ==
    \/ conf[m].epoch = conf[p].epoch
    \/ Bug = "accept_older_epoch" /\ conf[m].epoch > conf[p].epoch

(* The length of the longest common prefix of a member's log s and the   *)
(* primary's log t. Records compare by (epoch, seq); a Hole matches       *)
(* nothing.                                                                *)
CommonPrefixLen(s, t) ==
    LET n == Min(Len(s), Len(t))
        diff == {i \in 1..n : s[i] # t[i]}
    IN IF diff = {} THEN n ELSE SetMin(diff) - 1

NoHoles(s) == \A i \in 1..Len(s) : s[i] # Hole

(* Writes a new configuration to the register. The compare-and-swap     *)
(* succeeds only over the epoch the proposer read, expected.              *)
CAS(expected, new) ==
    /\ new.epoch <= MaxEpoch
    /\ reg.epoch = expected \/ Bug = "blind_register_write"
    /\ reg' = new

-----------------------------------------------------------------------------
(* Initial state: epoch 1 with the initial members, empty logs, and the   *)
(* initial primary serving. Spares know the configuration and are not in  *)
(* it. No lease has been granted yet.                                      *)

Init ==
    /\ reg = InitConfig
    /\ conf = [n \in Nodes |-> InitConfig]
    /\ log = [n \in Nodes |-> <<>>]
    /\ commit = [n \in Nodes |-> 0]
    /\ pstate = [n \in Nodes |-> IF n = InitPrimary THEN "serving" ELSE "backup"]
    /\ synced = [n \in Nodes |-> IF n = InitPrimary THEN InitMembers \ {n} ELSE {}]
    /\ ackd = [n \in Nodes |-> ZeroMap]
    /\ ackSet = [n \in Nodes |-> {}]
    /\ promoting = [n \in Nodes |-> NoPromotion]
    /\ leaseLeft = [n \in Nodes |-> ZeroMap]
    /\ graceLeft = [n \in Nodes |-> 0]
    /\ stepdowns = {}
    /\ nextVal = 1
    /\ restarts = 0
    /\ committed = {}
    /\ committers = {}
    /\ observed = 0
    /\ readsOK = TRUE

-----------------------------------------------------------------------------
(* Time *)

(* One real tick. Every node's clock advances by a rate in Rates, and its *)
(* timers run down in its own local units.                                *)
Tick ==
    \E r \in [Nodes -> Rates] :
        /\ leaseLeft' = [p \in Nodes |->
                            [m \in Nodes |-> Max(0, leaseLeft[p][m] - r[p])]]
        /\ graceLeft' = [n \in Nodes |-> Max(0, graceLeft[n] - r[n])]
        /\ UNCHANGED <<regVars, nodeVars, primaryVars, envVars, historyVars>>

-----------------------------------------------------------------------------
(* Write path (section 5.1) *)

(* The primary assigns the next seq to a client write and appends it to   *)
(* its own log. It accepts writes only while enough copies acknowledge    *)
(* (section 6.4).                                                          *)
ClientWrite(p) ==
    /\ pstate[p] = "serving" /\ conf[p].primary = p
    /\ nextVal <= MaxWrites
    /\ Cardinality(conf[p].members \cup ackSet[p]) >= MinWriteReplicas
    /\ log' = [log EXCEPT ![p] = Append(@, [ep |-> conf[p].epoch, val |-> nextVal])]
    /\ nextVal' = nextVal + 1
    /\ UNCHANGED <<regVars, conf, commit, pstate, primaryVars, timeVars,
                   stepdowns, restarts, historyVars>>

(* The primary sends m its next record. m checks the epoch (R2) and that *)
(* the seq follows its last record, appends, and acknowledges once the   *)
(* record is durable. Delivery, fsync, and acknowledgement are one step:  *)
(* a delayed or lost message is a step taken later or never, and a        *)
(* deposed primary keeps sending until it learns of the new epoch, which *)
(* covers stragglers. Members also learn the commit watermark from        *)
(* appends; the model does not track it at members, because only a       *)
(* primary serves, and a new primary recomputes it by reconciling.        *)
Replicate(p, m) ==
    /\ IsPrimary(p)
    /\ m \in AckGroup(p) /\ m \in synced[p]
    /\ EpochOK(p, m)
    /\ pstate[m] = "backup"
    /\ Len(log[m]) < Len(log[p])
    /\ LET i == Len(log[m]) + 1 IN
        /\ log' = [log EXCEPT ![m] = Append(@, log[p][i])]
        /\ ackd' = [ackd EXCEPT ![p][m] = i]
    /\ UNCHANGED <<regVars, conf, commit, pstate, synced, ackSet, promoting,
                   timeVars, envVars, historyVars>>

(* The commit rule: a record commits when every member of the primary's  *)
(* configuration, and every learner in its acknowledgement set, has      *)
(* acknowledged it. A commit at a serving primary acknowledges the        *)
(* client. A reconciling primary rolls the tail forward without           *)
(* acknowledging anyone (section 6.6).                                    *)
CommitOK(p, i) ==
    IF Bug = "commit_on_majority"
    THEN LET have == {m \in conf[p].members :
                        m = p \/ (m \in synced[p] /\ ackd[p][m] >= i)}
         IN 2 * Cardinality(have) > Cardinality(conf[p].members)
    ELSE \A m \in AckGroup(p) : m \in synced[p] /\ ackd[p][m] >= i

Commit(p) ==
    /\ IsPrimary(p)
    /\ LET ok == {i \in (commit[p] + 1)..Len(log[p]) : CommitOK(p, i)} IN
        /\ ok # {}
        /\ LET i == SetMax(ok) IN
            /\ commit' = [commit EXCEPT ![p] = i]
            /\ committed' = committed \cup {<<j, log[p][j]>> : j \in 1..i}
            /\ committers' = committers \cup {[epoch |-> conf[p].epoch, node |-> p]}
            /\ observed' = IF pstate[p] \in {"serving", "steppedDown"}
                           THEN Max(observed, i) ELSE observed
    /\ UNCHANGED <<regVars, conf, log, pstate, primaryVars, timeVars, envVars,
                   readsOK>>

-----------------------------------------------------------------------------
(* Reads and leases (section 5.4) *)

(* A primary serves a strongly consistent read only while it holds a     *)
(* valid lease from every member. The read returns the state at its      *)
(* commit watermark. Any node that believes it is the serving primary may *)
(* be asked: that is what a gateway with an arbitrarily stale shard map  *)
(* can do, and the epoch a gateway sends can only make the node reject   *)
(* the read. A read is linearizable if it returns at least the newest    *)
(* state a client has already seen.                                        *)
Read(p) ==
    /\ conf[p].primary = p
    /\ \/ pstate[p] = "serving"
       \/ Bug = "stepped_down_keeps_reading" /\ pstate[p] = "steppedDown"
    /\ \A m \in LeaseGroup(p) : leaseLeft[p][m] > 0
    /\ readsOK' = (readsOK /\ commit[p] >= observed)
    /\ observed' = Max(observed, commit[p])
    /\ UNCHANGED <<regVars, nodeVars, primaryVars, timeVars, envVars,
                   committed, committers>>

(* A lease round: the primary's beacon, m's acknowledgement, and the     *)
(* grant, as one step (see the module header). m grants the lease only   *)
(* as a backup in the primary's epoch. The lease lasts Lease on the      *)
(* primary's clock; m restarts its primary_grace timer on its own clock.  *)
(* Learners in the acknowledgement set grant leases as members do, so a  *)
(* primary can hold a lease from a learner it promotes.                   *)
Beacon(p, m) ==
    /\ conf[p].primary = p /\ pstate[p] \in {"reconciling", "serving"}
    /\ m # p /\ m \in conf[p].members \cup ackSet[p] \cup Promoting(p)
    /\ conf[m].epoch = conf[p].epoch
    /\ GrantsLeases(m)
    /\ leaseLeft' = [leaseLeft EXCEPT ![p][m] = Lease]
    /\ graceLeft' = [graceLeft EXCEPT ![m] = Grace]
    /\ UNCHANGED <<regVars, nodeVars, primaryVars, envVars, historyVars>>

-----------------------------------------------------------------------------
(* Configuration changes (section 6.3) *)

(* The configurations a node can learn of: the register's, and any other *)
(* node's, carried by its messages and redirect hints.                    *)
KnownConfigs == {reg} \cup {conf[p] : p \in Nodes}

(* A node adopts a newer configuration it learns of and appends it as    *)
(* its CONFIG record before it acts in the new epoch. Configurations may *)
(* arrive in any order and skip epochs, but a node only moves forward.   *)
Adopt(n, c) ==
    /\ c.epoch > conf[n].epoch
    /\ conf' = [conf EXCEPT ![n] = c]
    /\ IF c.primary = n /\ conf[n].primary = n
          /\ pstate[n] \in {"reconciling", "serving", "steppedDown"}
       THEN \* The same primary after a member removal, or after a learner
            \* was added or promoted. Acknowledgements carry over.
            /\ synced' = [synced EXCEPT ![n] = @ \cap (c.members \cup c.learners)]
            /\ ackSet' = [ackSet EXCEPT ![n] = @ \cap c.learners]
            /\ promoting' = [promoting EXCEPT ![n] =
                                IF @.epoch > 0 /\ c.epoch >= @.epoch
                                THEN NoPromotion ELSE @]
            /\ UNCHANGED <<commit, pstate, ackd, leaseLeft>>
       ELSE \* A takeover this node proposed (R1: nobody else can write
            \* one naming it), or another node's configuration.
            /\ pstate' = [pstate EXCEPT ![n] =
                            IF c.primary = n THEN "reconciling" ELSE "backup"]
            /\ commit' = [commit EXCEPT ![n] = 0]
            /\ synced' = [synced EXCEPT ![n] = {}]
            /\ ackSet' = [ackSet EXCEPT ![n] = {}]
            /\ promoting' = [promoting EXCEPT ![n] = NoPromotion]
            /\ ackd' = [ackd EXCEPT ![n] = ZeroMap]
            /\ leaseLeft' = [leaseLeft EXCEPT ![n] = ZeroMap]
    /\ UNCHANGED <<reg, log, graceLeft, envVars, historyVars>>

(* Member removal (section 6.4), by the primary or the coordinator: a CAS *)
(* to epoch e+1 without a member other than the primary. Any member may  *)
(* be suspected at any time.                                               *)
RemoveMember(m) ==
    /\ m \in reg.members /\ m # reg.primary
    /\ CAS(reg.epoch, [reg EXCEPT !.epoch = @ + 1, !.members = @ \ {m}])
    /\ UNCHANGED <<nodeVars, primaryVars, timeVars, envVars, historyVars>>

(* The coordinator adds a learner (section 6.7). The node may be a former *)
(* member that still holds an old log for the shard (re-admission).       *)
AddLearner(l) ==
    /\ l \notin reg.members \cup reg.learners
    /\ CAS(reg.epoch, [reg EXCEPT !.epoch = @ + 1, !.learners = @ \cup {l}])
    /\ UNCHANGED <<nodeVars, primaryVars, timeVars, envVars, historyVars>>

-----------------------------------------------------------------------------
(* Primary takeover (sections 5.4, 6.5) *)

(* A member of the configuration it knows takes over once primary_grace *)
(* has passed since it last granted a lease, or once the primary has      *)
(* stepped down to it in this epoch. R1: it stops acknowledging its epoch *)
(* and granting leases, records its proposal durably, and CASes itself in *)
(* as primary of epoch e+1 over the configuration it knows, keeping or    *)
(* dropping the old primary. Stopping and proposing are one step: a gap   *)
(* between them only removes acknowledgements and grants. If the register *)
(* has moved, the CAS fails and nothing changes; the node follows the     *)
(* register when it adopts it. A candidate learns the outcome only when  *)
(* it adopts a newer configuration: a lost response is a delayed Adopt.  *)
ProposeTakeover(n) ==
    /\ pstate[n] = "backup"
    /\ n \in conf[n].members /\ n # conf[n].primary
    /\ \/ graceLeft[n] = 0
       \/ [to |-> n, epoch |-> conf[n].epoch] \in stepdowns
       \/ Bug = "serve_before_grace"
    /\ \E keepOld \in BOOLEAN :
        LET old == conf[n]
            new == [epoch |-> old.epoch + 1, primary |-> n,
                    members |-> IF keepOld THEN old.members
                                ELSE old.members \ {old.primary},
                    learners |-> old.learners]
        IN CAS(old.epoch, new)
    /\ pstate' = [pstate EXCEPT ![n] = "proposed"]
    /\ UNCHANGED <<conf, log, commit, primaryVars, timeVars, envVars,
                   historyVars>>

(* Reconciliation (section 6.6) and learner catch-up (section 6.7). The   *)
(* primary collects m's last (epoch, seq), and m writes a TRUNCATE for    *)
(* every record past the longest prefix it shares with the primary's log. *)
(* The primary then re-replicates its log past that point (Replicate),   *)
(* which rolls its uncommitted tail forward. For a re-admitted learner,   *)
(* the same check verifies the (epoch, seq) prefix of its old records.    *)
Sync(p, m) ==
    /\ IsPrimary(p) /\ pstate[p] # "steppedDown"
    /\ m # p /\ m \in (conf[p].members \cup conf[p].learners) \ synced[p]
    /\ conf[m].epoch = conf[p].epoch
    /\ pstate[m] = "backup"
    /\ LET k == IF Bug = "truncate_by_seq_only"
                THEN Min(Len(log[m]), Len(log[p]))
                ELSE CommonPrefixLen(log[m], log[p])
       IN /\ log' = [log EXCEPT ![m] = SubSeq(@, 1, k)]
          /\ ackd' = [ackd EXCEPT ![p][m] = k]
    /\ synced' = [synced EXCEPT ![p] = @ \cup {m}]
    /\ UNCHANGED <<regVars, conf, commit, pstate, ackSet, promoting, timeVars,
                   envVars, historyVars>>

(* The new primary serves once every member holds its log and the whole *)
(* log is committed.                                                       *)
FinishReconcile(p) ==
    /\ pstate[p] = "reconciling" /\ conf[p].primary = p
    /\ conf[p].members \ {p} \subseteq synced[p]
    /\ commit[p] = Len(log[p])
    /\ pstate' = [pstate EXCEPT ![p] = "serving"]
    /\ UNCHANGED <<regVars, conf, log, commit, primaryVars, timeVars, envVars,
                   historyVars>>

-----------------------------------------------------------------------------
(* Planned handoff (section 5.4) *)

(* The primary stops serving reads and writes and stops renewing leases, *)
(* durably, then sends the candidate a step-down message for its epoch.  *)
StepDown(p, c) ==
    /\ pstate[p] = "serving" /\ conf[p].primary = p
    /\ c \in conf[p].members \ {p}
    /\ pstate' = [pstate EXCEPT ![p] = "steppedDown"]
    /\ stepdowns' = stepdowns \cup {[to |-> c, epoch |-> conf[p].epoch]}
    /\ UNCHANGED <<regVars, conf, log, commit, primaryVars, timeVars, nextVal,
                   restarts, historyVars>>

-----------------------------------------------------------------------------
(* Learners (sections 6.4, 6.7) *)

(* Live stream first: once the primary has verified the learner's prefix, *)
(* the learner joins the acknowledgement set and stores new records from *)
(* the primary's current seq on. The records in between are holes until  *)
(* backfill copies them.                                                   *)
JoinAckSet(p, l) ==
    /\ pstate[p] = "serving" /\ conf[p].primary = p
    /\ l \in conf[p].learners \cap synced[p] /\ l \notin AckGroup(p)
    /\ conf[l].epoch = conf[p].epoch /\ pstate[l] = "backup"
    /\ log' = [log EXCEPT ![l] =
                  @ \o [i \in 1..(Len(log[p]) - Len(@)) |-> Hole]]
    /\ ackSet' = [ackSet EXCEPT ![p] = @ \cup {l}]
    /\ ackd' = [ackd EXCEPT ![p][l] = Len(log[p])]
    /\ UNCHANGED <<regVars, conf, commit, pstate, synced, promoting, timeVars,
                   envVars, historyVars>>

(* Backfill copies the learner's lowest missing record.                   *)
Backfill(p, l) ==
    /\ IsPrimary(p) /\ l \in synced[p] /\ l \in conf[p].learners
    /\ conf[l].epoch = conf[p].epoch /\ pstate[l] = "backup"
    /\ ~NoHoles(log[l])
    /\ LET i == SetMin({j \in 1..Len(log[l]) : log[l][j] = Hole}) IN
        log' = [log EXCEPT ![l][i] = log[p][i]]
    /\ UNCHANGED <<regVars, conf, commit, pstate, primaryVars, timeVars,
                   envVars, historyVars>>

(* A learner that misses member_suspect_after leaves the acknowledgement *)
(* set without a CAS, and must catch up again before it rejoins. The      *)
(* learner of an outstanding promotion is never dropped: the primary     *)
(* keeps waiting for it until it knows the outcome of the CAS.            *)
DropLearner(p, l) ==
    /\ IsPrimary(p) /\ l \in ackSet[p]
    /\ l \notin Promoting(p) \/ Bug = "drop_promoting_learner"
    /\ ackSet' = [ackSet EXCEPT ![p] = @ \ {l}]
    /\ synced' = [synced EXCEPT ![p] = @ \ {l}]
    /\ UNCHANGED <<regVars, nodeVars, ackd, promoting, timeVars, envVars,
                   historyVars>>

(* R3: the primary promotes a learner by CAS once the learner is in the  *)
(* acknowledgement set, has finished backfill, and is durable up to the  *)
(* commit watermark. It records the proposal durably first, and keeps    *)
(* waiting for the learner and requiring its lease until it adopts a     *)
(* configuration at or past the proposed epoch. Commits do not pause.    *)
Promote(p, l) ==
    /\ pstate[p] = "serving" /\ conf[p].primary = p
    /\ promoting[p].epoch = 0
    /\ l \in conf[p].learners \cap ackSet[p]
    /\ \/ Bug = "promote_before_watermark"
       \/ NoHoles(log[l]) /\ Len(log[l]) >= commit[p]
    /\ LET old == conf[p]
           new == [old EXCEPT !.epoch = @ + 1, !.members = @ \cup {l},
                              !.learners = @ \ {l}]
       IN /\ CAS(old.epoch, new)
          /\ promoting' = [promoting EXCEPT ![p] = [node |-> l, epoch |-> new.epoch]]
    /\ UNCHANGED <<nodeVars, synced, ackd, ackSet, timeVars, envVars,
                   historyVars>>

-----------------------------------------------------------------------------
(* Restarts *)

(* A node restarts with its durable state: its log, its CONFIG record, a *)
(* takeover or promotion it has proposed, and a step-down. It loses its  *)
(* leases, its view of other nodes' acknowledgements, and when it last   *)
(* granted a lease, so it counts the restart as a grant and waits         *)
(* primary_grace before taking over. A primary re-aligns its members and *)
(* recommits before it serves again.                                       *)
Restart(n) ==
    /\ restarts < MaxRestarts
    /\ restarts' = restarts + 1
    /\ pstate' = [pstate EXCEPT ![n] =
          CASE @ = "proposed" /\ Bug = "restart_forgets_proposal" -> "backup"
            [] @ = "steppedDown" /\ Bug = "restart_forgets_step_down" -> "reconciling"
            [] @ \in {"proposed", "steppedDown"} -> @
            [] @ \in {"reconciling", "serving"} -> "reconciling"
            [] OTHER -> "backup"]
    /\ commit' = [commit EXCEPT ![n] = 0]
    /\ graceLeft' = [graceLeft EXCEPT ![n] =
                        IF Bug = "restart_forgets_grace" THEN 0 ELSE Grace]
    /\ synced' = [synced EXCEPT ![n] = {}]
    /\ ackSet' = [ackSet EXCEPT ![n] = {}]
    /\ ackd' = [ackd EXCEPT ![n] = ZeroMap]
    /\ leaseLeft' = [leaseLeft EXCEPT ![n] = ZeroMap]
    /\ UNCHANGED <<regVars, conf, log, promoting, stepdowns, nextVal,
                   historyVars>>

-----------------------------------------------------------------------------

Next ==
    \/ Tick
    \/ \E n \in Nodes :
        \/ ClientWrite(n)
        \/ Commit(n)
        \/ Read(n)
        \/ ProposeTakeover(n)
        \/ FinishReconcile(n)
        \/ Restart(n)
        \/ RemoveMember(n)
        \/ AddLearner(n)
    \/ \E p, m \in Nodes :
        \/ Replicate(p, m)
        \/ Sync(p, m)
        \/ Beacon(p, m)
        \/ StepDown(p, m)
        \/ JoinAckSet(p, m)
        \/ Backfill(p, m)
        \/ DropLearner(p, m)
        \/ Promote(p, m)
    \/ \E n \in Nodes, c \in KnownConfigs : Adopt(n, c)

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Invariants *)

TypeOK ==
    /\ reg \in Config
    /\ conf \in [Nodes -> Config]
    /\ log \in [Nodes -> Seq(Record \cup {Hole})]
    /\ commit \in [Nodes -> 0..MaxWrites]
    /\ pstate \in [Nodes -> PStates]
    /\ synced \in [Nodes -> SUBSET Nodes]
    /\ ackd \in [Nodes -> [Nodes -> 0..MaxWrites]]
    /\ ackSet \in [Nodes -> SUBSET Nodes]
    /\ leaseLeft \in [Nodes -> [Nodes -> 0..Lease]]
    /\ graceLeft \in [Nodes -> 0..Grace]
    /\ nextVal \in 1..(MaxWrites + 1)
    /\ restarts \in 0..MaxRestarts
    /\ observed \in 0..MaxWrites
    /\ readsOK \in BOOLEAN

(* Committed records survive: every committed record is at its seq in the *)
(* log of every member of the current configuration, and so of every     *)
(* node that can become primary (R1).                                     *)
CommittedRecordsSurvive ==
    \A r \in committed : \A n \in reg.members :
        r[1] <= Len(log[n]) /\ log[n][r[1]] = r[2]

(* No two primaries commit different records at the same seq.            *)
CommittedRecordsAgree ==
    \A r, s \in committed : r[1] = s[1] => r[2] = s[2]

(* One committing primary per epoch.                                      *)
OneCommitterPerEpoch ==
    \A a, b \in committers : a.epoch = b.epoch => a.node = b.node

(* Reads are linearizable: no read returns state older than a write that *)
(* was already acknowledged, or than another read already returned.      *)
ReadsLinearizable == readsOK

(* The initial backups are interchangeable, and so are the spares.       *)
Symmetry ==
    {s @@ t : s \in Permutations(InitMembers \ {InitPrimary}),
              t \in Permutations(Nodes \ InitMembers)}

=============================================================================
