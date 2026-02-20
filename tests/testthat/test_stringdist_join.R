context("stringdist_join")

# setup
d <- tibble::tibble(
  cut2 = c("Idea", "Premiums", "Premiom", "VeryGood", "VeryGood", "Faiir")
  ) %>%
  dplyr::mutate(type = dplyr::row_number())

if (!requireNamespace("ggplot2", quietly = TRUE)) {
  testthat::skip("ggplot2 not installed")
}

diamonds <- tibble::as_tibble(ggplot2::diamonds)

test_that("stringdist_inner_join works on a large df with multiples in each", {
  # create something with names close to the cut column in the diamonds dataset
  j <- stringdist_inner_join(diamonds, d, by = c(cut = "cut2"), distance_col = "distance")

  result <- j %>%
    dplyr::count(cut, cut2) %>%
    dplyr::arrange(cut)

  expect_equal(as.character(result$cut), c("Fair", "Very Good", "Premium", "Premium", "Ideal"))
  expect_equal(result$cut2, c("Faiir", "VeryGood", "Premiom", "Premiums", "Idea"))

  expect_equal(sum(j$cut == "Premium"), sum(diamonds$cut == "Premium") * 2)
  expect_equal(sum(j$cut == "Very Good"), sum(diamonds$cut == "Very Good") * 2)
  expect_equal(sum(j$cut2 == "Premiom"), sum(diamonds$cut == "Premium"))
  expect_true(all(j$distance == 1))

  vg <- j %>%
    dplyr::filter(cut == "Very Good") %>%
    dplyr::count(type)

  expect_equal(vg$type, c(4, 5))
  expect_equal(vg$n, rep(sum(diamonds$cut == "Very Good"), 2))

  expect_true(all(j$type[j$cut == "Faiir"] == 1))
})


d2 <- head(d, 3)
included <- c("Ideal", "Premium")
notin <- c("Fair", "Good", "Very Good")


test_that("stringdist_left_join works as expected", {
  result <- diamonds %>%
    stringdist_left_join(d2, by = c(cut = "cut2"))

  expect_true(all(is.na(result$cut2[result$cut %in% notin])))
  expect_equal(sum(result$cut %in% notin), sum(diamonds$cut %in% notin))

  expect_equal(sum(result$cut2 == "Premiom", na.rm = TRUE),
               sum(diamonds$cut == "Premium"))
  expect_equal(sum(result$cut2 == "Premiom", na.rm = TRUE),
               sum(result$cut2 == "Premiums", na.rm = TRUE))
})


d3 <- dplyr::bind_rows(d2, tibble::tibble(cut2 = "NewType", type = 4))

test_that("stringdist_right_join works as expected", {
  result <- diamonds %>%
    stringdist_right_join(d3, by = c(cut = "cut2"))

  expect_equal(sum(result$cut2 == "NewType"), 1)
  expect_equal(sum(is.na(result$cut)), 1)
  expect_true(all(is.na(result$cut[result$cut2 == "NewType"])))

  expect_equal(sum(result$cut2 == "Premiom", na.rm = TRUE),
               sum(diamonds$cut == "Premium"))
  expect_equal(sum(result$cut2 == "Premiom", na.rm = TRUE),
               sum(result$cut2 == "Premiums", na.rm = TRUE))
})


test_that("stringdist_full_join works as expected", {
  result <- diamonds %>%
    stringdist_full_join(d3, by = c(cut = "cut2"))

  expect_equal(sum(result$cut2 == "NewType", na.rm = TRUE), 1)
  expect_equal(sum(is.na(result$cut)), 1)
  expect_true(all(is.na(result$cut[result$cut2 == "NewType"])))

  expect_true(all(is.na(result$cut2[result$cut %in% notin])))
  expect_equal(sum(result$cut %in% notin), sum(diamonds$cut %in% notin))

  expect_equal(sum(result$cut2 == "Premiom", na.rm = TRUE),
               sum(diamonds$cut == "Premium"))
  expect_equal(sum(result$cut2 == "Premiom", na.rm = TRUE),
               sum(result$cut2 == "Premiums", na.rm = TRUE))
})



test_that("stringdist_semi_join works as expected", {
  result <- diamonds %>%
    stringdist_semi_join(d2, by = c(cut = "cut2"))

  expect_equal(sort(as.character(unique(result$cut))), included)

  expect_equal(nrow(result), sum(result$cut %in% included))

  expect_true(!("cut2" %in% colnames(result)))
})



test_that("stringdist_anti_join works as expected", {
  result <- diamonds %>%
    stringdist_anti_join(d2, by = c(cut = "cut2"))

  expect_equal(sort(as.character(unique(result$cut))), notin)

  expect_equal(nrow(result), sum(result$cut %in% notin))
})


test_that("stringdist_inner_join works with multiple match functions", {
  # setup
  d3 <- tibble::tibble(
    cut2 = c(
      "Idea", "Premiums", "Premiom",
      "VeryGood", "VeryGood", "Faiir"
    ),
    carat2 = c(0, .5, 1, 1.5, 2, 2.5)
  ) %>%
    dplyr::mutate(type = dplyr::row_number())

  sdist <- function(s1, s2) stringdist::stringdist(s1, s2) <= 1
  ndist <- function(n1, n2) abs(n1 - n2) < .25

  j <- diamonds %>%
    fuzzy_inner_join(d3, by = c(cut = "cut2", carat = "carat2"),
                     match_fun = list(sdist, ndist))

  result <- j %>%
    dplyr::count(cut, cut2)

  expect_equal(as.character(result$cut), c("Fair", "Very Good", "Premium", "Premium", "Ideal"))
  expect_equal(result$cut2, c("Faiir", "VeryGood", "Premiom", "Premiums", "Idea"))

  expect_lt(max(abs(j$carat - j$carat2)), .25)

  # give match_fun as a named list
  j_named <- diamonds %>%
    fuzzy_inner_join(d3, by = c(cut = "cut2", carat = "carat2"),
                     match_fun = list(carat = ndist, cut = sdist))

  expect_equal(j, j_named)
})


test_that("stringdist_join works with data frames without matches", {
  d <- tibble(cut2 = c(
    "Ideolll", "Premiumsss", "Premiomzzz",
    "VeryVeryGood", "VeryVeryGood", "FaiirsFair"
  )) %>%
    dplyr::mutate(type = dplyr::row_number())

  j1 <- stringdist_inner_join(diamonds, d, by = c(cut = "cut2"))
  expect_equal(nrow(j1), 0)
  expect_true(all(c("carat", "cut", "cut2", "type") %in% colnames(j1)))

  # check it works when column names are the same
  d2 <-dplyr::rename(d, cut = cut2)
  j1_5 <- stringdist_inner_join(diamonds, d2, by = c(cut = "cut"))
  expect_equal(nrow(j1_5), 0)
  expect_true(all(c("carat", "cut.x", "cut.y", "type") %in% colnames(j1_5)))

  j2 <- stringdist_left_join(diamonds, d, by = c(cut = "cut2"))
  expect_equal(nrow(j2), nrow(diamonds))
  expect_true(all(is.na(j2$cut2)))

  j3 <- stringdist_right_join(diamonds, d, by = c(cut = "cut2"))
  expect_equal(nrow(j3), nrow(d))
  expect_true(all(is.na(j3$carat)))
  expect_true(all(is.na(j3$cut)))

  j4 <- stringdist_full_join(diamonds, d, by = c(cut = "cut2"))
  expect_equal(nrow(j4), nrow(diamonds) + nrow(d))
  expect_true(all(is.na(j4$cut) | is.na(j4$cut2)))
  expect_true(all(is.na(j4$carat) | is.na(j4$type)))

  j5 <- stringdist_semi_join(diamonds, d, by = c(cut = "cut2"))
  expect_equal(nrow(j5), 0)
  expect_true(!("cut2" %in% colnames(diamonds)))
  expect_true(!("type" %in% colnames(diamonds)))
  expect_true("cut" %in% colnames(diamonds))
  expect_true("carat" %in% colnames(diamonds))

  j6 <- stringdist_anti_join(diamonds, d, by = c(cut = "cut2"))
  expect_equal(nrow(j6), nrow(diamonds))
  expect_true("cut" %in% colnames(diamonds))
  expect_true(!("cut2" %in% colnames(diamonds)))
})


test_that("stringdist_join can ignore case", {
  d_lowercase <- d %>%
    dplyr::mutate(cut2 = stringr::str_to_lower(cut2))

  # no matches generally
  j1 <- stringdist_inner_join(diamonds, d_lowercase, by = c(cut = "cut2"), distance_col = "distance",
                              max_dist = 1)
  expect_equal(nrow(j1), 0)

  # but with case ignored...
  j2 <- stringdist_inner_join(diamonds, d_lowercase, by = c(cut = "cut2"),
                              distance_col = "distance",
                              ignore_case = TRUE, max_dist = 1)
  expect_gt(nrow(j2), 0)
  expect_equal(sum(j2$cut == "Premium"), sum(diamonds$cut == "Premium") * 2)
  expect_true(all(j2$distance[j2$cut2 == "idea"] == 1))
})


test_that("stringdist_join can use soundex matching", {
  j <- stringdist_inner_join(diamonds, d, by = c(cut = "cut2"), distance_col = "distance",
                             method = "soundex")

  expect_gt(nrow(j), 0)
  expect_equal(sum(j$cut == "Premium"), sum(diamonds$cut == "Premium") * 2)
})


test_that("stringdist_join renames similar columns", {
  d <- tibble::tibble(cut = c(
    "Idea", "Premiums", "Premiom",
    "VeryGood", "VeryGood", "Faiir"
  )) %>%
    dplyr::mutate(price = dplyr::row_number())

  j <- stringdist_inner_join(diamonds, d, by = "cut")

  expect_true("cut.x" %in% colnames(j))
  expect_true("price.x" %in% colnames(j))
  expect_true("cut.y" %in% colnames(j))
  expect_true("price.y" %in% colnames(j))

  expect_true(all(j$cut.y %in% d$cut))
  expect_true(all(j$price.y %in% d$price))
})

test_that(paste("stringdist_join returns a data.frame when x",
                "is a data.frame, whether y is or not"), {
  result <- diamonds %>%
    as.data.frame() %>%
    stringdist_inner_join(d, by = c(cut = "cut2"))

  expect_is(result, "data.frame")
  expect_false(inherits(result, "tbl_df"))

  result <- diamonds %>%
    as.data.frame() %>%
    stringdist_inner_join(as.data.frame(d), by = c(cut = "cut2"))

  expect_is(result, "data.frame")
  expect_false(inherits(result, "tbl_df"))
})

test_that("stringdist_join works on grouped data frames", {
  d <- tibble::tibble(cut2 = c(
    "Idea", "Premiums", "Premiom",
    "VeryGood", "VeryGood", "Faiir"
  )) %>%
    dplyr::mutate(type = dplyr::row_number())

  diamonds_grouped <- diamonds %>%
    dplyr::group_by(cut)

  d2 <- tibble::tibble(cut = c(
    "Idea", "Premiums", "Premiom",
    "VeryGood", "VeryGood", "Faiir"
  )) %>%
    dplyr::mutate(type = dplyr::row_number())

  for (mode in c("inner", "left", "right", "full", "semi", "anti")) {
    j1 <- stringdist_join(diamonds, d, by = c(cut = "cut2"), mode = mode)
    j2 <- stringdist_join(diamonds_grouped, d, by = c(cut = "cut2"), mode = mode)

    expect_equal(length(groups(j2)), 1)
    expect_equal(as.character(groups(j2)[[1]]), "cut")
    expect_equal(j1, ungroup(j2))

    j3 <- stringdist_join(diamonds_grouped, d2, by = "cut", mode = mode)
    expect_is(j3, "tbl_df")
    expect_equal(length(groups(j3)), 1)
    expect_equal(nrow(j1), nrow(j3))
    if (mode %in% c("semi", "anti")) {
      expect_equal(as.character(groups(j3)[[1]]), "cut")
    } else {
      expect_equal(as.character(groups(j3)[[1]]), "cut.x")
    }
  }
})

test_that("stringdist_join works on one-column data.frames", {
  # this tests for (the fix of) the bug reported in #13
  d <- data.frame(cut2 = c("Idea", "Premiums", "Premiom",
                           "VeryGood", "VeryGood", "Faiir"))

  diamonds <- as.data.frame(diamonds)
  result <- stringdist_inner_join(diamonds, d, by = c(cut = "cut2"))
  expect_is(result, "data.frame")
  expect_true("cut2" %in% colnames(result))
  expect_gt(nrow(result), 0)
})


test_that("stringdist fails with no common variables", {
  expect_error(stringdist_inner_join(diamonds, d),
               "No common variables")
})

test_that("stringdist_ joins where there are no overlapping rows still get a distance column", {
  a <- tibble::tibble(x = c("apple", "banana"))
  b <- tibble::tibble(y = c("orange", "mango"))

  result <- stringdist_left_join(a, b, by = c(x = "y"), max_dist = 1, distance_col = "distance")

  expect_equal(colnames(result), c("x", "y", "distance"))
  expect_equal(nrow(result), 2)
  expect_true(all(is.na(result$y)))
  expect_true(all(is.na(result$distance)))

  result <- stringdist_inner_join(a, b, by = c(x = "y"), max_dist = 1, distance_col = "distance")

  expect_equal(colnames(result), c("x", "y", "distance"))
  expect_equal(nrow(result), 0)

  # Don't add it for semi or anti join
  result <- stringdist_semi_join(a, b, by = c(x = "y"), max_dist = 1, distance_col = "distance")
  expect_equal(colnames(result), "x")
  expect_equal(nrow(result), 0)

  result <- stringdist_anti_join(a, b, by = c(x = "y"), max_dist = 1, distance_col = "distance")
  expect_equal(a, result)
})
