---
"@effect-grpc/effect-grpc": patch
---

`refreshingTokenLayer`'s daemon now repeats the refresh through
`Schedule.spaced` instead of a manual `Effect.sleep` + `Schedule.forever`
loop, matching Effect's own scoped daemons, and runs interruptibly so layer
shutdown is never held by the daemon's wait. The first refresh cycle runs
when the layer is built rather than one interval later.
