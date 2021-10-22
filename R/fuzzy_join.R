#' Join two tables based not on exact matches, but with a function
#' describing whether two vectors are matched or not
#'
#' The \code{match_fun} argument is called once on a vector with all pairs
#' of unique comparisons: thus, it should be efficient and vectorized.
#'
#' @param x A tbl
#' @param y A tbl
#' @param by Columns of each to join
#' @param match_fun Vectorized function given two columns, returning
#' TRUE or FALSE as to whether they are a match. Can be a list of functions
#' one for each pair of columns specified in \code{by} (if a named list, it
#' uses the names in x).
#' If only one function is given it is used on all column pairs.
#' @param multi_by Columns to join, where all columns will be used to
#' test matches together
#' @param multi_match_fun Function to use for testing matches, performed
#' on all columns in each data frame simultaneously
#' @param index_match_fun Function to use for matching tables. Unlike
#' \code{match_fun} and \code{index_match_fun}, this is performed on the
#' original columns and returns pairs of indices.
#' @param mode One of "inner", "left", "right", "full" "semi", or "anti"
#' @param ... Extra arguments passed to match_fun
#'
#' @details match_fun should return either a logical vector, or a data
#' frame where the first column is logical. If the latter, the
#' additional columns will be appended to the output. For example,
#' these additional columns could contain the distance metrics that
#' one is filtering on.
#'
#' Note that as of now, you cannot give both \code{match_fun}
#' and \code{multi_match_fun}- you can either compare each column
#' individually or compare all of them.
#'
#' Like in dplyr's join operations, \code{fuzzy_join} ignores groups,
#' but preserves the grouping of x in the output.
#'
#' @importFrom dplyr %>%
#' @importFrom tibble tibble
#'
#' @export
fuzzy_join <- function(x, y, by = NULL, match_fun = NULL,
                       multi_by = NULL, multi_match_fun = NULL,
                       index_match_fun = NULL, mode = "inner", ...) {
  # preserve the grouping of x
  x_groups <- dplyr::groups(x)
  x <- dplyr::ungroup(x)
  regroup <- function(d) {
    if (length(x_groups) == 0) {
      return(d)
    }

    g <- purrr::map_chr(x_groups, as.character)
    missing <- !(g %in% colnames(d))
    # add .x to those that are missing; they've been renamed
    g[missing] <- paste0(g[missing], ".x")

    dplyr::group_by_at(d, g)
  }

  mode <- match.arg(mode, c("inner", "left", "right", "full", "semi", "anti"))

  match_fun_type <- c("match_fun", "multi_match_fun", "index_match_fun")[
    c(!is.null(match_fun), !is.null(multi_match_fun), !is.null(index_match_fun))
  ]
  if (length(match_fun_type) > 1) {
    stop("Must give exactly one of match_fun, multi_match_fun, and index_match_fun")
  }

  matches <- switch(
    match_fun_type,
    match_fun = get_matches(x, y, by, match_fun, ...),
    multi_match_fun = get_matches_multi(x, y, by, multi_match_fun, multi_by),
    index_match_fun = get_matches_index(x, y, by, index_match_fun, multi_by)
  )
  matches$i <- NULL

  if (mode == "semi") {
    # just use the x indices to include
    return(regroup(x[sort(unique(matches$x)), , drop = FALSE]))
  }
  if (mode == "anti") {
    if (nrow(matches) == 0) {
      return(regroup(x))
    }
    # just use the x indices to exclude
    return(regroup(x[-sort(unique(matches$x)), , drop = FALSE]))
  }

  matches <- dplyr::arrange(matches, x, y)

  # in cases where columns share a name, rename each to .x and .y
  n <- intersect(colnames(x), colnames(y))
  x <- dplyr::rename_at(x, .vars = n, ~ paste0(.x, ".x"))
  y <- dplyr::rename_at(y, .vars = n, ~ paste0(.x, ".y"))

  # fill in indices of the x, y, or both
  # curious if there's a higher performance approach
  if (mode == "left") {
    matches <- tibble::tibble(x = seq_len(nrow(x))) %>%
      dplyr::left_join(matches, by = "x")
  } else if (mode == "right") {
    matches <- tibble::tibble(y = seq_len(nrow(y))) %>%
      dplyr::left_join(matches, by = "y")
  } else if (mode == "full") {
    matches <- matches %>%
      dplyr::full_join(tibble::tibble(x = seq_len(nrow(x))), by = "x") %>%
      dplyr::full_join(tibble::tibble(y = seq_len(nrow(y))), by = "y")
  }

  ret <- dplyr::bind_cols(
    unrowwname(x[matches$x, , drop = FALSE]),
    unrowwname(y[matches$y, , drop = FALSE])
  )
  if (ncol(matches) > 2) {
    extra_cols <- unrowwname(matches[, -(1:2), drop = FALSE])
    ret <- dplyr::bind_cols(ret, extra_cols)
  }

  ret <- regroup(ret)

  # Base the type (data.frame vs tbl_df) on x, not on y
  if (!inherits(x, "tbl_df")) {
    ret <- as.data.frame(ret)
  }

  ret
}


#' @rdname fuzzy_join
#' @export
fuzzy_inner_join <- function(x, y, by = NULL, match_fun, ...) {
  fuzzy_join(x, y, by, match_fun, mode = "inner", ...)
}


#' @rdname fuzzy_join
#' @export
fuzzy_left_join <- function(x, y, by = NULL, match_fun, ...) {
  fuzzy_join(x, y, by, match_fun, mode = "left", ...)
}


#' @rdname fuzzy_join
#' @export
fuzzy_right_join <- function(x, y, by = NULL, match_fun, ...) {
  fuzzy_join(x, y, by, match_fun, mode = "right", ...)
}


#' @rdname fuzzy_join
#' @export
fuzzy_full_join <- function(x, y, by = NULL, match_fun, ...) {
  fuzzy_join(x, y, by, match_fun, mode = "full", ...)
}


#' @rdname fuzzy_join
#' @export
fuzzy_semi_join <- function(x, y, by = NULL, match_fun, ...) {
  fuzzy_join(x, y, by, match_fun, mode = "semi", ...)
}


#' @rdname fuzzy_join
#' @export
fuzzy_anti_join <- function(x, y, by = NULL, match_fun, ...) {
  fuzzy_join(x, y, by, match_fun, mode = "anti", ...)
}
