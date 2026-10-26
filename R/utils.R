"%||%" <- function(x, y) if (is.null(x)) y else x


common_by <- function(by = NULL, x, y) {
  if (is.list(by)) return(by)

  if (!is.null(by)) {
    x <- names(by) %||% by
    y <- unname(by)

    # If x partially named, assume unnamed are the same in both tables
    x[x == ""] <- y[x == ""]

    return(list(x = x, y = y))
  }

  by <- intersect(dplyr::tbl_vars(x), dplyr::tbl_vars(y))
  if (length(by) == 0) {
    stop("No common variables. Please specify `by` param.", call. = FALSE)
  }
  message("Joining by: ", utils::capture.output(dput(by)))

  list(
    x = by,
    y = by
  )
}

# Make sure there's a distance column included in the output. Joins on
# multiple columns already add one per column (e.g. x1.distance), so skip those.
ensure_distance_col <- function(ret, distance_col, mode) {
  if (!(mode %in% c("semi", "anti")) &&
      !is.null(distance_col) &&
      is.null(ret[[distance_col]]) &&
      !any(endsWith(colnames(ret), paste0(".", distance_col)))) {
    if (nrow(ret) == 0) {
      ret[[distance_col]] <- numeric(0)
    } else {
      ret[[distance_col]] <- NA
    }
  }
  ret
}

unrowwname <- function(x) {
  rownames(x) <- NULL
  x
}
