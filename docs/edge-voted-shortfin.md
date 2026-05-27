# Edge-Voted Shortfin Baseline

This note records a baseline formalization for an optimistic Shortfin extension.
The goal is to turn next-round DAG edges into implicit votes for the previous
round leader, while keeping Shortfin's embedded-QC path as fallback.

## Model

Let there be `n = 3f + 1` validators, with at most `f` Byzantine validators.
Time is divided into DAG rounds. Each validator creates at most one vertex per
round.

For validator `i` in round `r`, define a vertex:

```text
v_i^r = <i, r, payload, parents, parent2, evidence>_sig_i
```

where:

```text
|parents(v_i^r)| >= 2f + 1      from round r - 1
|parent2(v_i^r)| >= 2f + 1      from round r - 2
```

Votes and certificates are bound to the full vertex digest:

```text
digest(v_i^r) = H(i, r, payload, parents, parent2, evidence)
```

This prevents a Byzantine author from reusing votes across different evidence
or parent sets.

Each round has a deterministic leader author `leader(r)`. If that author
produces a valid vertex, denote the leader vertex by:

```text
L_r = v_leader(r)^r
```

## Edge Votes

A round `r + 1` vertex implicitly supports the round `r` leader if it directly
references the leader as a parent:

```text
support(v_i^{r+1}, L_r) := L_r in parents(v_i^{r+1})
```

For the conservative baseline proof, only certified or otherwise globally
deliverable round `r + 1` vertices are counted in a fast certificate. This keeps
the fast path aligned with the certified DAG used by the fallback path. A more
aggressive first-header variant would need an additional Sailfish-style argument
that first-seen values eventually agree with delivered vertices.

The next round therefore induces an edge-vote certificate:

```text
FC(L_r) = { v_i^{r+1} | support(v_i^{r+1}, L_r) }
```

`L_r` is fast-certified if:

```text
|FC(L_r)| >= 2f + 1
```

## Fast Commit Rule

The optimistic fast path commits the round leader once the local DAG contains a
fast certificate for it:

```text
FastCommit(L_r) iff |FC(L_r)| >= 2f + 1
```

Committed output remains slot ordered. A replica outputs round `r` only after
all earlier leader slots have been decided, skipped, or committed by fallback.
The conservative implementation emits only the fast-certified leader:

```text
FastOutput(L_r) = [L_r]
```

The rest of the leader's causal past is left to the fallback collector. This is
deliberate: emitting a locally visible causal past on the fast path would make
the early output depend on delivery timing at each replica.

## Fallback

If the fast certificate for `L_r` does not appear, the protocol falls back to
the existing Shortfin embedded-QC path. In particular, Shortfin's same-author
QC chain can still certify and commit vertices:

```text
b_3 -> b_2 -> b_1
QC(b_3) embedded in b_2
QC(b_2) embedded in b_1
```

Fast commits and fallback commits must share one ordered-prefix log. Sailfin's
fallback therefore uses a leader-first deterministic order: selected round
leaders contained in a fallback batch are placed first by leader round, and all
remaining certificates follow Shortfin's `(round, digest)` order. This rule is
protocol-fixed; it does not depend on whether a particular replica already saw
the fast certificate. Once a vertex or slot is committed by either path, later
fallback ordering filters out already ordered vertices and respects the decided
slot prefix.

## Safety Intuition

### Lemma 1: Fast-certificate uniqueness

Two conflicting leader digests in the same round cannot both be fast-certified.
Suppose `L_r` and `L'_r` both had fast certificates. Their supporting sets would
each contain certified round `r + 1` vertices from `2f + 1` distinct authors.
Since `n = 3f + 1`, the two author sets intersect in at least `f + 1` validators:

```text
|Q intersect Q'| >= (2f + 1) + (2f + 1) - (3f + 1) = f + 1
```

At least one validator in the intersection is honest. An honest validator
creates and certifies at most one round `r + 1` vertex, and that vertex can
directly reference at most one digest for the round `r` leader author. Therefore
the honest validator cannot support both `L_r` and `L'_r`, contradiction.

### Lemma 2: Fast commits become causally stable

If `FastCommit(L_r)` holds, then every valid certified vertex in round `r + 2`
has a path to `L_r`.

Proof. `FastCommit(L_r)` gives a set `FC(L_r)` of `2f + 1` certified round
`r + 1` vertices that directly reference `L_r`. Any valid round `r + 2` vertex
has at least `2f + 1` parents from round `r + 1`. Two subsets of size `2f + 1`
inside a universe of `3f + 1` authors intersect in at least `f + 1` authors.
Hence the parent set of any valid round `r + 2` vertex intersects `FC(L_r)`.
At least one parent of the round `r + 2` vertex therefore has a direct edge to
`L_r`, so the round `r + 2` vertex has a path to `L_r`.

By induction, every valid certified vertex in every later round also has a path
to `L_r`: each later vertex has at least one parent from the previous round, and
all previous-round certified vertices already have a path to `L_r`.

### Lemma 3: Fallback cannot contradict a fast commit

Assume `L_r` is fast-committed. Any fallback commit that occurs at round
`r + 2` or later commits an anchor whose causal past contains `L_r`, by Lemma 2.
Therefore the fallback batch can contain `L_r`. Since fallback always places
selected round leaders first by leader round, a replica that did not take the
fast path still emits the same leader prefix when it later falls back.

Fallback commits before round `r + 2` are either earlier-slot decisions, which
must appear before slot `r`, or decisions for already ordered vertices. They do
not create a conflicting value for slot `r` because Lemma 1 prevents a second
fast certificate for a conflicting leader digest, and Shortfin's embedded-QC
fallback continues to rely on full-digest quorum certificates.

### Safety theorem

No two honest replicas output different ordered prefixes.

Sketch. A fast decision for a slot is unique by Lemma 1. Once such a decision
exists, later fallback anchors inherit it causally by Lemma 2, and the
leader-first fallback rule reproduces the same leader prefix even at replicas
that did not take the fast path. If no fast decision exists, the protocol uses
the Shortfin embedded-QC fallback for that portion of the log, with the only
ordering change being the protocol-fixed leader-first tie-breaker over the same
certified DAG. The shared ordered-prefix rule then composes the two paths: fast
decisions become prefix constraints, and fallback fills unresolved portions
without reordering decided vertices.

Safety also requires:

- votes bind the full vertex digest;
- equivocation by the same author and round is detected as distinct digests;
- fast certificates count distinct authors, not raw vertices;
- fast certificates count certified or globally deliverable support vertices;
- fast-path and fallback decisions feed the same prefix-ordered output rule;
- fallback leader-first ordering is protocol-fixed, not local-evidence dependent;
- fallback cannot reorder or replace a slot already decided by the fast path.

## Liveness Intuition

### Optimistic fast-path liveness

After GST, suppose the round `r` leader is honest, its vertex is certified in
time, and honest validators use a parent-selection rule that includes the
current round leader when it is available. Then all `2f + 1` honest validators
can create certified round `r + 1` vertices that include `L_r` as a parent.
These vertices form `FC(L_r)`, so `FastCommit(L_r)` eventually holds.

The fast path adds no extra vote phase: the support is carried by normal
round `r + 1` DAG edges.

### Fallback liveness

If the leader is faulty, slow, or not observed by enough validators, the fast
certificate may not form. In that case, liveness is inherited from Shortfin's
embedded-QC fallback path rather than relying on the edge-vote path alone.

The fast path must therefore be non-blocking:

- validators try to include an available round leader in their parents;
- validators do not wait forever for a missing leader;
- absence of `FC(L_r)` never prevents normal Shortfin round advancement;
- fallback can decide unresolved slots or anchors according to the existing
  embedded-QC rule.

### Liveness theorem

After GST, honest replicas continue deciding.

Sketch. In good rounds with timely honest leaders, the optimistic edge-vote path
decides the leader after a next-round quorum. In bad rounds, validators keep
advancing the DAG and the protocol reduces to Shortfin's embedded-QC fallback,
which provides progress under its original synchrony and quorum assumptions.
Because fast decisions only add prefix constraints that every later certified
vertex inherits by Lemma 2, they do not block fallback progress.

## Main Claim

Edge-Voted Shortfin changes the optimization target from certificate
dissemination latency to commit latency. Shortfin removes the standalone
certificate broadcast; Edge-Voted Shortfin additionally asks whether the next
DAG round can serve as an implicit voting layer for every-round leader commit.

In the optimistic case, a leader can be committed after one next-round quorum
of edges, while the original embedded-QC chain remains available for recovery
and worst-case progress.
