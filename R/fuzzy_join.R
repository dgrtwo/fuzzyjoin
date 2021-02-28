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
#' @importFrom tibble tibble as_tibble
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

  non_nulls <- (!is.null(multi_match_fun)) +
    (!is.null(match_fun)) +
    (!is.null(index_match_fun))
  if (sum(non_nulls) != 1) {
    stop("Must give exactly one of match_fun, multi_match_fun, and index_match_fun")
  }

  if (!is.null(match_fun)) {
    by <- common_by(by, x, y)

    # Support formula notation for functions
    if (is.list(match_fun)) {
      match_fun <- purrr::map(match_fun, purrr::as_mapper)
    } else {
      match_fun <- purrr::as_mapper(match_fun)
    }

    if (length(match_fun) == 1) {
      match_fun <- rep(c(match_fun), length(by$x))
    }
    if (length(match_fun) != length(by$x)) {
      stop("Length of match_fun not equal to columns specified in 'by'.", call. = FALSE)
    }

    matches <- dplyr::bind_rows(lapply(seq_along(by$x), function(i) {
      col_x <- x[[by$x[i]]]
      col_y <- y[[by$y[i]]]

      indices_x <- tibble::tibble(
        col = col_x,
        indices = seq_along(col_x)
      ) %>%
        dplyr::group_by(col) %>%
        tidyr::nest() %>%
        dplyr::mutate(indices = purrr::map(data, "indices"))

      indices_y <- tibble::tibble(
        col = col_y,
        indices = seq_along(col_y)
      ) %>%
        dplyr::group_by(col) %>%
        tidyr::nest() %>%
        dplyr::mutate(indices = purrr::map(data, "indices"))

      u_x <- indices_x$col
      u_y <- indices_y$col

      if (!is.null(names(match_fun))) {
        # match_fun is a named list, use the names in x
        mf <- match_fun[[by$x[[i]]]]
      } else {
        mf <- match_fun[[i]]
      }

      extra_cols <- NULL

      n_x <- length(u_x)
      n_y <- length(u_y)
      m <- mf(rep(u_x, n_y), rep(u_y, each = n_x), ...)

      if (is.data.frame(m)) {
        if (ncol(m) > 1) {
          # first column is logical, others are included as distance columns
          extra_cols <- m[, -1, drop = FALSE]
        }
        m <- m[[1]]
      }

      # return as a data frame of x and y indices that match
      w <- which(m) - 1

      if (length(w) == 0) {
        # there are no matches
        ret <- tibble::tibble(i = numeric(0), x = numeric(0), y = numeric(0))
        return(ret)
      }

      x_indices_l <- indices_x$indices[w %% n_x + 1]
      y_indices_l <- indices_y$indices[w %/% n_x + 1]

      xls <- sapply(x_indices_l, length)
      yls <- sapply(y_indices_l, length)

      x_rep <- unlist(purrr::map2(x_indices_l, yls, function(x, y) rep(x, each = y)))
      y_rep <- unlist(purrr::map2(y_indices_l, xls, function(y, x) rep(y, x)))

      ret <- tibble::tibble(i = i, x = x_rep, y = y_rep)

      if (!is.null(extra_cols)) {
        extra_indices <- rep(w, xls * yls)
        extra_cols_rep <- extra_cols[extra_indices + 1, , drop = FALSE]
        ret <- dplyr::bind_cols(ret, extra_cols_rep)
      }

      ret
    }))

    if (length(by$x) > 1) {
      # only take cases where all pairs have matches
      accept <- matches %>%
        dplyr::count(x, y) %>%
        dplyr::ungroup() %>%
        dplyr::filter(n == length(by$x))

      matches <- matches %>%
        dplyr::semi_join(accept, by = c("x", "y"))

      if (ncol(matches) > 3) {
        # include one for each
        matches <- matches %>%
          dplyr::semi_join(accept, by = c("x", "y")) %>%
          dplyr::mutate(name = by$x[i]) %>%
          dplyr::select(-i) %>%
          tidyr::gather(key, value, -x, -y, -name) %>%
          tidyr::unite(newname, name, key, sep = ".") %>%
          tidyr::spread(newname, value)
      } else {
        matches <- dplyr::distinct(matches, x, y)
      }
    }
  } else if (!is.null(multi_match_fun)) {
    multi_match_fun <- purrr::as_mapper(multi_match_fun)

    # use multiple matches
    by <- common_by(multi_by, x, y)

    number_x_rows <- nrow(x)
    number_y_rows <- nrow(y)

    indices_x <- x %>%
      dplyr::select_at(by$x) %>%
      dplyr::mutate(indices = seq_len(number_x_rows)) %>%
      dplyr::group_by_at(dplyr::vars(-dplyr::one_of("indices"))) %>%
      tidyr::nest() %>%
      dplyr::mutate(indices = purrr::map(data, "indices"))
    indices_y <- y %>%
      dplyr::select_at(by$y) %>%
      dplyr::mutate(indices = seq_len(number_y_rows)) %>%
      dplyr::group_by_at(dplyr::vars(-dplyr::one_of("indices"))) %>%
      tidyr::nest() %>%
      dplyr::mutate(indices = purrr::map(data, "indices"))

    ux <- as.matrix(indices_x[by$x])
    uy <- as.matrix(indices_y[by$y])

    pairs <- matrix(NA, nrow(ux), nrow(uy))
    ix <- row(pairs)
    iy <- col(pairs)
    ux_input <- ux[ix, ]
    uy_input <- uy[iy, ]

    m <- multi_match_fun(ux_input, uy_input)

    extra_cols <- NULL
    if (is.data.frame(m)) {
      if (ncol(m) > 1) {
        extra_cols <- m[, -1, drop = FALSE]
      }
      m <- m[[1]]
    }

    if (sum(m) == 0) {
      # there are no matches
      matches <- tibble::tibble(x = numeric(0), y = numeric(0))
    } else {
      x_indices_l <- indices_x$indices[ix[m]]
      y_indices_l <- indices_y$indices[iy[m]]
      xls <- purrr::map_dbl(x_indices_l, length)
      yls <- purrr::map_dbl(y_indices_l, length)
      x_rep <- unlist(purrr::map2(x_indices_l, yls, function(x, y) rep(x, each = y)))
      y_rep <- unlist(purrr::map2(y_indices_l, xls, function(y, x) rep(y, x)))

      matches <- tibble::tibble(x = x_rep, y = y_rep)
      if (!is.null(extra_cols)) {
        extra_indices <- rep(which(m), xls * yls)
        extra_cols_rep <- extra_cols[extra_indices, , drop = FALSE]
        matches <- dplyr::bind_cols(matches, extra_cols_rep)
      }
    }
  } else {
    # raw index-index function
    index_match_fun <- purrr::as_mapper(index_match_fun)
    by <- common_by(multi_by, x, y)

    d1 <- x[, by$x, drop = FALSE]
    d2 <- y[, by$y, drop = FALSE]

    matches <- index_match_fun(d1, d2)
  }
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
