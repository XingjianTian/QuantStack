# Brokerage sample fixtures

Every identifier, symbol, balance, order, timestamp, and quote in this
directory is fictional.  These JSON files model the response shapes consumed by
`schwab-py` 1.5.1; they are not exports from a brokerage account.

The normal set exercises a margin account with a fractional equity position,
an option position whose contract multiplier is intentionally unknown, a
partially filled order, a trade transaction, and read-only quotes.  The other
files are negative cases for missing funding fields and stale quotes.

`normal_coverage.json` is an explicit sample coverage declaration.  It carries
the source observation time and states that the account, positions, orders,
transactions, fills, and quotes are complete and not truncated.  Missing
coverage metadata or missing provider timestamps is treated as an unusable
snapshot; the adapter never substitutes the local receive time for it.
