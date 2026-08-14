# retina-tools

Helper tools and scripts supporting the [Retina](https://github.com/dioptra-io) active
measurement platform. Not part of the core Retina system itself — this repo holds
standalone tooling (Go and shell) that Retina's pipelines depend on.

## Contents

- **`tier1exclusions/`** — Go tool that extracts, for each tier-1 ASN, its announced
  address blocks minus the sub-blocks belonging to other networks (customer prefixes).
  Produces the exclusion lists used to build ClickHouse `IP_TRIE` dictionaries for
  Retina's responsible-probing target space. See `tier1exclusions/` for build/run
  instructions.
- **`clickhouse/`** — scripts for loading exclusion lists into ClickHouse (planned).
- **`cron/`** — scheduling wrappers for the tools above (planned).

## License

MIT — see `LICENSE`.
