get_matches <- function(x, y, by, match_fun, ...) {
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

  # for each pair of key columns build a match data frame, and bind them
  matches <- purrr::map_dfr(seq_along(by$x), get_matches1, x, y, by, match_fun, ...)

  if (length(by$x) == 1) {
    return(matches)
  }

  # only take cases where all pairs have matches
  accept <- matches %>%
    dplyr::count(x, y) %>%
    dplyr::ungroup() %>%
    dplyr::filter(n == length(by$x))

  matches <- matches %>%
    dplyr::semi_join(accept, by = c("x", "y"))

  if (ncol(matches) == 3) {
    return(dplyr::distinct(matches, x, y))
  }

  # include one for each
  matches <- matches %>%
    dplyr::semi_join(accept, by = c("x", "y")) %>%
    dplyr::mutate(name = by$x[i]) %>%
    dplyr::select(-i) %>%
    tidyr::gather(key, value, -x, -y, -name) %>%
    tidyr::unite(newname, name, key, sep = ".") %>%
    tidyr::spread(newname, value)

  matches
}

get_matches_multi <- function(x, y, by, multi_match_fun, multi_by) {
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
  matches
}


get_matches_index <- function(x, y, by, index_match_fun, multi_by) {
  # raw index-index function
  index_match_fun <- purrr::as_mapper(index_match_fun)
  by <- common_by(multi_by, x, y)

  d1 <- x[, by$x, drop = FALSE]
  d2 <- y[, by$y, drop = FALSE]

  matches <- index_match_fun(d1, d2)
  matches
}


get_matches1 <- function(i, x, y, by, match_fun, ...) {
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
}
