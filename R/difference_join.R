#' Join two tables based on absolute difference between their columns
#'
#' @param x A tbl.
#' @param y A tbl.
#' @param by Columns by which to join the two tables.
#' @param max_dist Maximum distance to use for joining.
#' @param distance_col If given, will add a column with this name containing the
#'   difference between the two.
#' @param mode One of "inner", "left", "right", "full" "semi", or "anti"
#'
#' @examples
#'
#' library(dplyr)
#'
#' head(iris)
#' sepal_lengths <- tibble::tibble(Sepal.Length = c(5, 6, 7), Type = 1:3)
#'
#' iris %>%
#'   difference_inner_join(sepal_lengths, max_dist = .5)
#'
#' @export
difference_join <- function(x, y, by = NULL, max_dist = 1, mode = "inner",
                            distance_col = NULL) {
  match_fun <- function(v1, v2) {
    dist <- abs(v1 - v2)
    ret <- data.frame(include = (dist <= max_dist))
    if (!is.null(distance_col)) {
      ret[[distance_col]] <- dist
    }
    ret
  }

  ensure_distance_col(fuzzy_join(x, y, by = by, match_fun = match_fun, mode = mode), distance_col, mode)
}


#' @rdname difference_join
#' @export
difference_inner_join <- function(x, y, by = NULL, max_dist = 1, distance_col = NULL) {
  difference_join(x, y, by, max_dist = max_dist, mode = "inner", distance_col = distance_col)
}


#' @rdname difference_join
#' @export
difference_left_join <- function(x, y, by = NULL, max_dist = 1, distance_col = NULL) {
  difference_join(x, y, by, max_dist = max_dist, mode = "left", distance_col = distance_col)
}


#' @rdname difference_join
#' @export
difference_right_join <- function(x, y, by = NULL, max_dist = 1, distance_col = NULL) {
  difference_join(x, y, by, max_dist = max_dist, mode = "right", distance_col = distance_col)
}


#' @rdname difference_join
#' @export
difference_full_join <- function(x, y, by = NULL, max_dist = 1, distance_col = NULL) {
  difference_join(x, y, by, max_dist = max_dist, mode = "full", distance_col = distance_col)
}


#' @rdname difference_join
#' @export
difference_semi_join <- function(x, y, by = NULL, max_dist = 1, distance_col = NULL) {
  difference_join(x, y, by, max_dist = max_dist, mode = "semi", distance_col = distance_col)
}


#' @rdname difference_join
#' @export
difference_anti_join <- function(x, y, by = NULL, max_dist = 1, distance_col = NULL) {
  difference_join(x, y, by, max_dist = max_dist, mode = "anti", distance_col = distance_col)
}
