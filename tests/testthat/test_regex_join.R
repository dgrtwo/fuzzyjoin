context("regex_join")

# setup
d <- tibble::tibble(cut_regex = c("^Idea", "emiu",
                          "Very Good$", "Nowhere")) %>%
  dplyr::mutate(type = dplyr::row_number())

test_that("regex joins work", {
  testthat::skip_if_not_installed("ggplot2")
  diamonds <- tibble::as_tibble(ggplot2::diamonds)
  j <- diamonds %>%
    regex_inner_join(d, by = c(cut = "cut_regex"))

  expect_equal(nrow(j), sum(diamonds$cut %in% c("Ideal", "Premium", "Very Good")))
  expect_equal(sort(unique(j$type)), 1:3)
  expect_true(all(j$type[j$cut == "Ideal"] == 1))
  expect_false("Nowhere" %in% j$cut_regex)

  j2 <- diamonds %>%
    regex_left_join(d, by = c(cut = "cut_regex"))

  expect_equal(nrow(diamonds), nrow(j2))
  expect_equal(sum(is.na(j2$type)), sum(diamonds$cut %in% c("Fair", "Good")))
  expect_true(all(j$cut[is.na(j$type)] %in% c("Fair", "Good")))

  j3 <- diamonds %>%
    regex_right_join(d, by = c(cut = "cut_regex"))

  expect_equal(nrow(j3), sum(diamonds$cut %in% c("Ideal", "Premium", "Very Good")) + 1)
  expect_true("Nowhere" %in% j3$cut_regex)

  j4 <- diamonds %>%
    regex_full_join(d, by = c(cut = "cut_regex"))

  expect_equal(nrow(j4), nrow(diamonds) + 1)
  expect_true("Nowhere" %in% j4$cut_regex)

  j5 <- diamonds %>%
    regex_semi_join(d, by = c(cut = "cut_regex"))

  expect_equal(j5, diamonds[diamonds$cut %in% c("Ideal", "Premium", "Very Good"), ])

  j6 <- diamonds %>%
    regex_anti_join(d, by = c(cut = "cut_regex"))

  expect_equal(j6, diamonds[!diamonds$cut %in% c("Ideal", "Premium", "Very Good"), ])
})
