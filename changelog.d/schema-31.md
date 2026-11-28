Changed

- **Opening a datadir rewrites store `meta` to schema 31.** Schema 31 adds
  page-guard marks under `store/guard/` and changes no table. A 0.9 binary
  refuses a datadir this build has opened.
