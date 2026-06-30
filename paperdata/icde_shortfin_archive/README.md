# Legacy ICDE Shortfin Archive Metadata

This directory is kept for compatibility with the earlier archive
layout. The canonical data entry point is now:

```text
paperdata/README.md
```

The compact WAN summary text files that previously lived under
`cloud/summaries/` have been consolidated into:

```text
paperdata/summary/wan/
```

The normalized cloud CSV remains here:

```text
cloud/cloud_wan_summary.csv
```

Its `source_file` column points to the consolidated summary location.
The historical protocol label `noveldag` corresponds to Shortfin in
the manuscript.
