# Mutation evidence

All mutations edited real template, controller, process, or chart source. Each original file was restored by exact content replacement.
The final Rust and Docker gates passed after restoration. The complete coverage matrix is in [testing.md](testing.md).

## Docker

`STALEWISE_IMAGE=stalewise:tests python3 scripts/cache-mutations.py` reported `RED` for all 36 named mutants.
The script records assertion failures, not compilation failures. Every new Docker test group killed at least one mutant.

A separate process mutation replaced `config.storage.clone().into()` with `"malloc,256m".into()` in the Varnish child arguments.
The rebuilt mutant image failed `chunked_objects_cache_and_small_storage_evicts_without_crashing`: `oldest object was evicted`.
The 4 MiB configuration therefore guards actual storage pressure, not only successful delivery.

## Kubernetes

| Mutation | Observed failure |
|---|---|
| Disable PDB rendering | The named cache PodDisruptionBudget was not found. |
| Add update permission to the VirtualService Role | The update permission assertion expected `no` but received `yes`. |
| Permit TTL -1 in the CRD | `API server accepted a negative TTL` |
| Expose management Service on 9091 | Curl could not connect to cache-management on 9090. |
| Replace Active status reason with Pending | Accepted condition reason was Pending, not Active. |
| Disable equal-prefix conflict detection | Accepted remained True/Active instead of False/Conflict. |
| Remove source-loop attachment validation | Attached remained True instead of False/MissingSourceBypass. |
| Sort and truncate reconciled policies to nine | Acceptance timed out for the named policy `many-8`. |

The initial per-reconcile limit survived because unordered iteration eventually visited every policy.
The replacement deterministic capacity defect failed; the survivor was not counted as evidence.

## Rollout

Before the separately implemented drain fix, two unchanged runs failed with:

```text
curl: (56) Recv failure: Connection reset by peer
command terminated with exit code 56
```

After rebasing onto main `658cc0c`, two fresh kind clusters passed both suites, including continuous serving during rollout.
This red-to-green proof uses the original product defect rather than an artificial failure.
