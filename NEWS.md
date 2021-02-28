# fuzzyjoin 0.1.6.9000

* Replaces instances of `dplyr::tbl_df()` with `tibble::as_tibble()`, since
`tbl_df()` is deprecated as of dplyr 1.0.0.

# fuzzyjoin 0.1.6

* Updates to internals to make compatible with dplyr 1.0.0 (#67, @hadley)

# fuzzyjoin 0.1.5

* fuzzy_join now supports formula notation for `match_fun`, `multi_match_fun`, and `index_match_fun`.
* Many changes to internals to make compatible with newest versions of dplyr and tidyr (#58 and #60, @jennybc)
* difference, distance, geo and stringdist joins now add a distance column even if there are no overlapping rows (fixes #57)
* distance joins now support the case where there's only one column in common (fixes #43)
* Fixed typos in documentation (#40, @ChrisMuir and #53, @brooke-watson)

# fuzzyjoin 0.1.4

* Fixed failing test of `max_overlaps` in `interval_join`
* Used conditional testing for the IRanges package
* A few fixes to the README

# fuzzyjoin 0.1.3

* Added `interval_join`, which joins tables on cases where (start, end) intervals overlap between the two columns. This adds IRanges from Bioconductor to SUGGESTS.
* Added `genome_join`, which is a more specific case of `interval_join` that joins tables on based on (chromosome, start, end), where the chromosome must agree and (start, end) must overlap.
* Added `index_match_fun` argument to `fuzzy_join`, which handles functions (such as `interval_join` and `genome_join`) that operate on the original columns rather than all pairs of columns
* Fixed bug when matching multiple columns to the same column (#28)
* Fixed bug in which rows were sometimes duplicated when no distance column was specified (#21)
* Added more unit tests

# fuzzyjoin 0.1.2

* Fixed bug that failed when single column data frames (not tbl_dfs) were given (#13)
* Updated README for newest versions of dplyr and janeaustenr

# fuzzyjoin 0.1.1

## Features

* Added option not only to join based on a maximum distance but also to append a distance column, using the `distance_col` argument. This is available in `difference_join`, `distance_join`, `stringdist_join`, and `geo_join` (#10)

## Bug fixes

* Fixed to ignore groups while preserving the groups of x in the output (#11)
* Fixed to append `.x` and `.y` to all common columns, not just those in `by` (#12)

## Package management

* Added codecov to measure test coverage
* Added AppVeyor continuous integration
* Added Code of Conduct

# fuzzyjoin 0.1

* Initial draft of package for CRAN
