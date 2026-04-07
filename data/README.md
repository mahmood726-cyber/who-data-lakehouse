# Data Directories

- `raw/` keeps source-faithful snapshots.
- `bronze/` keeps lightly standardized tables.
- `silver/` keeps normalized outputs meant for analysis.
- `reference/` keeps source documentation and lookup tables.

These directories are git-ignored by default because full WHO pulls can become large quickly.
