---
"@effect-grpc/effect-grpc": patch
---

**Overlapping pulls in the stream bridge no longer duplicate messages or leak a
fiber past teardown.** The pull machine behind both pumps retains a single pull
fiber, so a `next()` issued while another was in flight forked a second pull and
overwrote that slot: `close()` then interrupted only the last fiber, and the
abandoned pull — with its interrupt cleanup — stayed pending forever. The
overlapping pulls also raced the shared pull and chunk iterator, so callers
could see the same element more than once and lose others — three concurrent
`next()` calls on a one-element stream that then fails all returned that same
element, and the failure never surfaced at all. Pulls are
now serialized behind a promise chain, which `close()` bypasses so teardown
still interrupts the pull in flight instead of queueing behind it. No transport
path reached this today, because connect-node drives both iterators
sequentially.
