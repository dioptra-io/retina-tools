# retina-tools

## About

`retina-tools` is a collection of standalone tools supporting the
[Retina](https://github.com/dioptra-io) active measurement platform — things
Retina's core services depend on without needing to ship themselves.

## Contents

- **`tier1exclusions/`** — Go tool that extracts, for each tier-1 ASN, its announced
  address blocks minus the sub-blocks belonging to other networks (customer
  prefixes). Produces the exclusion lists used to build ClickHouse `IP_TRIE`
  dictionaries for Retina's responsible-probing target space. See
  `tier1exclusions/` for build/run instructions.
- **`pipeline/`** — shell tools that consume `tier1exclusions`' output:
  `tier1_pipeline.sh` runs `tier1exclusions` and loads the result into ClickHouse;
  `load_tier1_prefixes.sh` does the ClickHouse loading specifically (staging tables,
  atomic swap into the live `IP_TRIE` dictionaries); `common.sh` holds shared
  logging/locking helpers.
- **`cron/`** — `tier1_wrapper.sh`, the thin scheduling layer that runs
  `pipeline/tier1_pipeline.sh` as an unattended monthly job (locking against
  overlapping runs, a small failure marker, pruning old output). `crontab.txt` has
  an example schedule.
- **`conf/`** — settings sourced by the `pipeline/`/`cron/` scripts
  (`common_settings.conf`, `tier1_settings.conf`).

## License

MIT — see `LICENSE`.