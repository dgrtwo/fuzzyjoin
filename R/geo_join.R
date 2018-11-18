#' Join two tables based on a geo distance of longitudes and latitudes
#'
#' This allows joining based on combinations of longitudes and latitudes. If
#' you are using a distance metric that is *not* based on latitude and
#' longitude, use \code{\link{distance_join}} instead. Distances are
#' calculated based on the \code{distHaversine}, \code{distGeo},
#' \code{distCosine}, etc methods in the geosphere package.
#'
#' @param x A tbl
#' @param y A tbl
#' @param by Columns by which to join the two tables. Must contain 'lon'
#'  for longitute, and 'lat' for latitude, such as 'lat', or
#'  'lat.y', or 'latitude'.
#' @param max_dist Maximum distance to use for joining
#' @param method Method to use for computing distance: one of
#' "haversine" (default), "geo", "cosine", "meeus", "vincentysphere",
#' "vincentyellipsoid"
#' @param unit Unit of distance for threshold (default "miles")
#' @param mode One of "inner", "left", "right", "full" "semi", or "anti"
#' @param distance_col If given, will add a column with this
#' name containing the geographical distance between the two
#' @param ... Extra arguments passed on to the distance method
#'
#' @details "Haversine" was chosen as default since in some tests it is
#' approximately the fastest method. Note that by far the slowest method is
#' vincentyellipsoid, and on fuzzy joins should only be used when there are
#' very few pairs and accuracy is imperative.
#'
#' If you need to use a custom geo method, you may want to write it directly
#' with the \code{multi_by} and \code{multi_match_fun} arguments to
#' \code{fuzzy_join}.
#'
#' @importFrom utils data
#' @importFrom dplyr common_by data_frame
#'
#' @examples
#'
#' library(dplyr)
#' data("state")
#'
#' # find pairs of US states whose centers are within
#' # 200 miles of each other
#' states <- data_frame(state = state.name,
#'                      longitude = state.center$x,
#'                      latitude = state.center$y)
#'
#' s1 <- rename(states, state1 = state)
#' s2 <- rename(states, state2 = state)
#'
#' pairs <- s1 %>%
#'  geo_inner_join(s2, max_dist = 200) %>%
#'  filter(state1 != state2)
#'
#' pairs
#'
#' # plot them
#' library(ggplot2)
#' ggplot(pairs, aes(x = longitude.x, y = latitude.x,
#'                   xend = longitude.y, yend = latitude.y)) +
#'   geom_segment(color = "red") +
#'   borders("state") +
#'   theme_void()
#'
#' # also get distances
#' s1 %>%
#'   geo_inner_join(s2, max_dist = 200, distance_col = "distance")
#'
#' @export
geo_join <- function(x, y, by = NULL, max_dist,
                     method = c("haversine", "geo", "cosine", "meeus",
                                "vincentysphere", "vincentyellipsoid"),
                     unit = c("miles", "km"),
                     mode = "inner",
                     distance_col = NULL,
                     ...) {
  method <- match.arg(method)
  unit <- match.arg(unit)

  # make sure longitude and latitude are in the right order
  by <- dplyr::common_by(by, x, y)
  by <- lapply(by, function(e) {
    if (length(e) != 2) {
      stop("Trying to join on ", paste(e, collapse = ", "),
           "; geo_join needs exactly two columns (latitude and longitude)")
    }
    firstthree <- stringr::str_extract(stringr::str_to_lower(e), "(lon|lat)")
    colmatches <- match(c("lon", "lat"), firstthree)

    if (any(is.na(colmatches)) || length(unique(colmatches)) != 2) {
      message("Could not determine which is lon/lat, using in given order")
      e
    } else {
      e[colmatches]
    }
  })

  match_fun <- function(v1, v2) {
    if (method == "geo") {
      d <- geosphere::distGeo(v1, v2, ...)
    } else if (method == "haversine") {
      d <- geosphere::distHaversine(v1, v2, ...)
    } else if (method == "cosine") {
      d <- geosphere::distCosine(v1, v2, ...)
    } else if (method == "meeus") {
      d <- geosphere::distMeeus(v1, v2, ...)
    } else if (method == "vincentysphere") {
      d <- geosphere::distVincentySphere(v1, v2, ...)
    } else if (method == "vincentyellipsoid") {
      d <- geosphere::distVincentyEllipsoid(v1, v2, ...)
    }

    if (unit == "miles") {
      d <- d / 1609.344
    } else {
      d <- d / 1000
    }

    ret <- dplyr::data_frame(include = d <= max_dist)
    if (!is.null(distance_col)) {
      ret[[distance_col]] <- d
    }
    ret
  }

  fuzzy_join(x, y, multi_by = by, multi_match_fun = match_fun, mode = mode)
}


#' @rdname geo_join
#' @export
geo_inner_join <- function(x, y, by = NULL, method = "haversine", max_dist = 1,
                           distance_col = NULL, ...) {
  geo_join(x, y, by, max_dist = max_dist, method = method, mode = "inner",
           distance_col = distance_col, ...)
}


#' @rdname geo_join
#' @export
geo_left_join <- function(x, y, by = NULL, method = "haversine", max_dist = 1,
                          distance_col = NULL, ...) {
  geo_join(x, y, by, max_dist = max_dist, method = method, mode = "left",
           distance_col = distance_col, ...)
}


#' @rdname geo_join
#' @export
geo_right_join <- function(x, y, by = NULL, method = "haversine", max_dist = 1,
                           distance_col = NULL, ...) {
  geo_join(x, y, by, max_dist = max_dist, method = method, mode = "right",
           distance_col = distance_col, ...)
}


#' @rdname geo_join
#' @export
geo_full_join <- function(x, y, by = NULL, method = "haversine", max_dist = 1,
                          distance_col = NULL, ...) {
  geo_join(x, y, by, max_dist = max_dist, method = method, mode = "full",
           distance_col = distance_col, ...)
}


#' @rdname geo_join
#' @export
geo_semi_join <- function(x, y, by = NULL, method = "haversine", max_dist = 1,
                          distance_col = NULL, ...) {
  geo_join(x, y, by, max_dist = max_dist, method = method, mode = "semi",
           distance_col = distance_col, ...)
}


#' @rdname geo_join
#' @export
geo_anti_join <- function(x, y, by = NULL, method = "haversine", max_dist = 1,
                          distance_col = NULL, ...) {
  geo_join(x, y, by, max_dist = max_dist, method = method, mode = "anti",
           distance_col = distance_col, ...)
}
