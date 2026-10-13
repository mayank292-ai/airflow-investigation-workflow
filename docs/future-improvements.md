# Future improvements

Current verification is in the matrix. A fresh post-restart report,
live repetitions, full counterexample suite and budget-stop behavior executed;
accepted reports still contain documented semantic defects.
The current v12 focused suite completed all12 fresh investigations without
integrity failures; varying coverage/recommendations remain in qualitative review.
Live watcher launch and reopened-process dedup verification passed: six complete
reports, with no new events or operations on the second process's scan.
Real Airflow scheduled history/restart and all five isolated patch checks passed.
Independent human semantic review, wider reliability evaluation and a clean lock
install remain unestablished. The user subsequently approved$15 total, resolving
the earlier$5 admission block without resetting any spending records.

After those gates pass:

- Collect more real, sanitized failure records and untouched evaluation variants.
  Keep unsuccessful runs and explicit human reviews; add deterministic fact
  comparisons where structured evidence makes them reliable.
- Add narrowly scoped BigQuery job/destination evidence: job statistics, query
  plans, partition/commit state and schema. Service tests are required before
  claiming BigQuery correctness or performance gains.
- Capture worker/container resource metrics and termination reasons. This can
  support stronger conclusions than SIGKILL alone, with timestamp alignment.
- Integrate Git/image/DAG-version provenance and real deployment manifests.
  Preserve exact imported modules and dynamic configuration when permissions
  allow, without exposing fault controls or secrets.
- Improve interruption recovery, external destination snapshots and detection of
  intermediate retry transitions. Do not turn at-least-once behavior into an
  exactly-once claim.
- Add human-approved recovery only with known business semantics, strict action
  permissions, precondition checks and independently verified post-action state.
- Consider context management or additional agents only after measurements
  identify a concrete need. Vector storage/queues are not currently necessary.
- Consider optional UI only after this CLI workflow is useful and verified.
  The existing Airflow interface and readable artifacts already support the demo.
