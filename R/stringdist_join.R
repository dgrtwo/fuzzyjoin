#' Join two tables based on fuzzy string matching of their columns
#'
#' Join two tables based on fuzzy string matching of their columns. This is
#' useful, for example, in matching free-form inputs in a survey or online form,
#' where it can catch misspellings and small personal changes.
#'
#' @param x A tbl.
#' @param y A tbl.
#' @param by Columns by which to join the two tables.
#' @param max_dist Maximum distance to use for joining.
#' @param ignore_case Whether to be case insensitive (default yes).
#' @param method Method for computing string distance, see.
#'   \code{stringdist-metrics} in the stringdist package.
#' @param distance_col If given, will add a column with this name containing the
#'   difference between the two.
#' @param mode One of "inner", "left", "right", "full" "semi", or "anti"
#' @param ... Arguments passed on to \code{\link[stringdist]{stringdist}}.
#'
#' @details If \code{method = "soundex"}, the \code{max_dist} is automatically
#'   set to 0.5, since soundex returns either a 0 (match) or a 1 (no match).
#'
#' @examples
#'
#' library(dplyr)
#' if (requireNamespace("ggplot2", quietly = TRUE)) {
#'   diamonds <- tibble::as_tibble(ggplot2::diamonds)
#'
#'   d <- tibble::tibble(approximate_name = c("Idea", "Premiums", "Premioom",
#'                                        "VeryGood", "VeryGood", "Faiir"),
#'                   type = 1:6)
#'
#'   # no matches when they are inner-joined:
#'   diamonds %>%
#'     inner_join(d, by = c(cut = "approximate_name"))
#'
#'   # but we can match when they're fuzzy joined
#'   diamonds %>%
#'    stringdist_inner_join(d, by = c(cut = "approximate_name"))
#' }
#'
#' @export
stringdist_join <- function(x, y, by = NULL, max_dist = 2,
                            method = c("osa", "lv", "dl", "hamming", "lcs", "qgram",
                                       "cosine", "jaccard", "jw", "soundex"),
                            mode = "inner",
                            ignore_case = FALSE,
                            distance_col = NULL,
                            ...) {
  method <- match.arg(method)

  if (method == "soundex") {
    # soundex always returns 0 or 1, so any other max_dist would
    # lead either to always matching or never matching
    max_dist <- .5
  }

  match_fun <- function(v1, v2) {
    if (ignore_case) {
      v1 <- stringr::str_to_lower(v1)
      v2 <- stringr::str_to_lower(v2)
    }

    # shortcut for Levenshtein-like methods: if the difference in
    # string length is greater than the maximum string distance, the
    # edit distance must be at least that large

    # length is much faster to compute than string distance
    if (method %in% c("osa", "lv", "dl")) {
      length_diff <- abs(stringr::str_length(v1) - stringr::str_length(v2))
      include <- length_diff <= max_dist

      dists <- rep(NA, length(v1))

      dists[include] <- stringdist::stringdist(v1[include], v2[include], method = method, ...)
    } else {
      # have to compute them all
      dists <- stringdist::stringdist(v1, v2, method = method, ...)
    }
    ret <- tibble::tibble(include = (dists <= max_dist))
    if (!is.null(distance_col)) {
      ret[[distance_col]] <- dists
    }
    ret
  }

  ensure_distance_col(fuzzy_join(x, y,
                                    by = by,
                                    mode = mode,
                                    match_fun = match_fun),
                        distance_col, mode)

}


#' @rdname stringdist_join
#' @export
stringdist_inner_join <- function(x, y, by = NULL, distance_col = NULL, ...) {
  stringdist_join(x, y, by, mode = "inner", distance_col = distance_col, ...)
}


#' @rdname stringdist_join
#' @export
stringdist_left_join <- function(x, y, by = NULL, distance_col = NULL, ...) {
  stringdist_join(x, y, by, mode = "left", distance_col = distance_col, ...)
}


#' @rdname stringdist_join
#' @export
stringdist_right_join <- function(x, y, by = NULL, distance_col = NULL, ...) {
  stringdist_join(x, y, by, mode = "right", distance_col = distance_col, ...)
}


#' @rdname stringdist_join
#' @export
stringdist_full_join <- function(x, y, by = NULL, distance_col = NULL, ...) {
  stringdist_join(x, y, by, mode = "full", distance_col = distance_col, ...)
}


#' @rdname stringdist_join
#' @export
stringdist_semi_join <- function(x, y, by = NULL, distance_col = NULL, ...) {
  stringdist_join(x, y, by, mode = "semi", distance_col = distance_col, ...)
}


#' @rdname stringdist_join
#' @export
stringdist_anti_join <- function(x, y, by = NULL, distance_col = NULL, ...) {
  stringdist_join(x, y, by, mode = "anti", distance_col = distance_col, ...)
}
