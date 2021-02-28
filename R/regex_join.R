#' Join two tables based on a regular expression in one column
#' matching the other
#'
#' Join a table with a string column by a regular expression column
#' in another table
#'
#' @param x A tbl
#' @param y A tbl
#' @param by Columns by which to join the two tables
#' @param mode One of "inner", "left", "right", "full" "semi", or "anti"
#' @param ignore_case Whether to be case insensitive (default no)
#'
#' @seealso \code{\link{str_detect}}
#'
#' @examples
#'
#' library(dplyr)
#' library(ggplot2)
#' data(diamonds)
#'
#' diamonds <- as_tibble(diamonds)
#'
#' d <- data_frame(regex_name = c("^Idea", "mium", "Good"),
#'                 type = 1:3)
#'
#' # When they are inner_joined, only Good<->Good matches
#' diamonds %>%
#'   inner_join(d, by = c(cut = "regex_name"))
#'
#' # but we can regex match them
#' diamonds %>%
#'  regex_inner_join(d, by = c(cut = "regex_name"))
#'
#' @export
regex_join <- function(x, y, by = NULL, mode = "inner", ignore_case = FALSE) {
  match_fun <- function(v1, v2) {
    stringr::str_detect(v1, stringr::regex(v2, ignore_case = ignore_case))
  }

  fuzzy_join(x, y, by = by, match_fun = match_fun, mode = mode)
}


#' @rdname regex_join
#' @export
regex_inner_join <- function(x, y, by = NULL, ignore_case = FALSE) {
  regex_join(x, y, by, mode = "inner", ignore_case = ignore_case)
}


#' @rdname regex_join
#' @export
regex_left_join <- function(x, y, by = NULL, ignore_case = FALSE) {
  regex_join(x, y, by, mode = "left", ignore_case = ignore_case)
}


#' @rdname regex_join
#' @export
regex_right_join <- function(x, y, by = NULL, ignore_case = FALSE) {
  regex_join(x, y, by, mode = "right", ignore_case = ignore_case)
}


#' @rdname regex_join
#' @export
regex_full_join <- function(x, y, by = NULL, ignore_case = FALSE) {
  regex_join(x, y, by, mode = "full", ignore_case = ignore_case)
}


#' @rdname regex_join
#' @export
regex_semi_join <- function(x, y, by = NULL, ignore_case = FALSE) {
  regex_join(x, y, by, mode = "semi", ignore_case = ignore_case)
}


#' @rdname regex_join
#' @export
regex_anti_join <- function(x, y, by = NULL, ignore_case = FALSE) {
  regex_join(x, y, by, mode = "anti", ignore_case = ignore_case)
}
