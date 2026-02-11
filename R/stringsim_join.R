#' Join two tables based on fuzzy string matching of their columns
#'
#' Join two tables based on fuzzy string matching of their columns.
#' This is useful, for example, in matching free-form inputs in
#' a survey or online form, where it can catch misspellings and
#' small personal changes.
#'
#' @param x A tbl
#' @param y A tbl
#' @param by Columns by which to join the two tables
#' @param min_sim Minimum similarity to use for joining
#' @param ignore_case Whether to be case insensitive (default yes)
#' @param method Method for computing string distance, see
#' \code{stringdist-metrics} in the stringdist package.
#' @param similarity_col If given, will add a column with this
#' name containing the difference between the two
#' @param mode One of "inner", "left", "right", "full" "semi", or "anti"
#' @param ... Arguments passed on to \code{\link{stringsim}}
#'
#' @details If \code{method = "soundex"}, the \code{min_sim} is
#' automatically set to 0.5, since soundex returns either a 0 (match)
#' or a 1 (no match).
#'
#' @examples
#'
#' library(dplyr)
#' library(ggplot2)
#' data(diamonds)
#'
#' d <- tibble(approximate_name = c("Idea", "Premiums", "Premioom",
#'                                  "VeryGood", "VeryGood", "Faiir"),
#'             type = 1:6)
#'
#' # no matches when they are inner-joined:
#' diamonds %>%
#'   mutate(cut = as.character(cut)) %>%
#'   inner_join(d, by = c(cut = "approximate_name"))
#'
#' # but we can match when they're fuzzy joined
#' diamonds %>%
#'   mutate(cut = as.character(cut)) %>%
#'   stringsim_inner_join(d, by = c(cut = "approximate_name"))
#'
#' @export
stringsim_join <- function(x, y, by = NULL, min_sim = 0.75,
                           method = c("osa", "lv", "dl", "hamming", "lcs", "qgram",
                                      "cosine", "jaccard", "jw", "soundex"),
                           mode = "inner",
                           ignore_case = FALSE,
                           similarity_col = NULL, ...) {
  method <- match.arg(method)

  if (method == "soundex") {
    # soundex always returns 0 or 1, so any other max_dist would
    # lead either to always matching or never matching
    min_sim <- .5
  }

  match_fun <- function(v1, v2) {
    if (ignore_case) {
      v1 <- stringr::str_to_lower(v1)
      v2 <- stringr::str_to_lower(v2)
    }

    sims <- stringdist::stringsim(v1, v2, method = method, ...)

    ret <- tibble::tibble(include = (sims >= min_sim))
    if (!is.null(similarity_col)) {
      ret[[similarity_col]] <- sims
    }
    ret
  }

  ensure_distance_col(fuzzy_join(x, y, by = by, mode = mode, match_fun = match_fun), similarity_col, mode)
}


#' @rdname stringsim_join
#' @export
stringsim_inner_join <- function(x, y, by = NULL, similarity_col = NULL, ...) {
  stringsim_join(x, y, by, mode = "inner", similarity_col = similarity_col, ...)
}


#' @rdname stringsim_join
#' @export
stringsim_left_join <- function(x, y, by = NULL, similarity_col = NULL, ...) {
  stringsim_join(x, y, by, mode = "left", similarity_col = similarity_col, ...)
}


#' @rdname stringsim_join
#' @export
stringsim_right_join <- function(x, y, by = NULL, similarity_col = NULL, ...) {
  stringsim_join(x, y, by, mode = "right", similarity_col = similarity_col, ...)
}


#' @rdname stringsim_join
#' @export
stringsim_full_join <- function(x, y, by = NULL, similarity_col = NULL, ...) {
  stringsim_join(x, y, by, mode = "full", similarity_col = similarity_col, ...)
}


#' @rdname stringsim_join
#' @export
stringsim_semi_join <- function(x, y, by = NULL, similarity_col = NULL, ...) {
  stringsim_join(x, y, by, mode = "semi", similarity_col = similarity_col, ...)
}


#' @rdname stringsim_join
#' @export
stringsim_anti_join <- function(x, y, by = NULL, similarity_col = NULL, ...) {
  stringsim_join(x, y, by, mode = "anti", similarity_col = similarity_col, ...)
}
