#' A corpus of common misspellings, for examples and practice
#'
#' This is a table mapping misspellings of their words, compiled by
#' Wikipedia, where it is licensed under the CC-BY SA license. (Three words with
#' non-ASCII characters were filtered out). If you'd like to reproduce this
#' dataset from Wikipedia, see the example code below.
#'
#' @examples
#'
#' \dontrun{
#' library(rvest)
#' library(readr)
#' library(dplyr)
#' library(stringr)
#' library(tidyr)
#'
#' u <- "https://en.wikipedia.org/wiki/Wikipedia:Lists_of_common_misspellings/For_machines"
#' h <- read_html(u)
#'
#' misspellings <- h %>%
#'   html_nodes("pre") %>%
#'   html_text() %>%
#'   readr::read_delim(col_names = c("misspelling", "correct"), delim = ">",
#'                     skip = 1) %>%
#'   mutate(misspelling = str_sub(misspelling, 1, -2)) %>%
#'   unnest(correct = str_split(correct, ", ")) %>%
#'   filter(Encoding(correct) != "UTF-8")
#' }
#'
#' @source
#'   \url{https://en.wikipedia.org/wiki/Wikipedia:Lists_of_common_misspellings/For_machines}
"misspellings"
