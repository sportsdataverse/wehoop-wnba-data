#!/usr/bin/env Rscript
# Run summary: parse the per-season tracked logs written by the daily processor
# and emit (a) a cli-formatted summary to stdout -- visible in the GitHub Action
# logs -- and (b) a markdown summary to $GITHUB_STEP_SUMMARY when set, so the run
# Summary tab shows which releases updated plus any remaining warnings/errors.
#
# Usage: Rscript R/run_summary.R -s 2025 -e 2025
# League + log prefix are auto-detected from logs/<prefix>_logfile_<year>.log.

suppressPackageStartupMessages({
  library(optparse)
  library(cli)
  library(glue)
})

option_list <- list(
  make_option(c("-s", "--start_year"), type = "integer", default = NA),
  make_option(c("-e", "--end_year"), type = "integer", default = NA)
)
opt <- parse_args(OptionParser(option_list = option_list))

# --- detect the data-log prefix (e.g. hoopR_nba_data / hoopR_mbb_data) --------
all_logs <- list.files("logs", pattern = "_data_logfile_[0-9]{4}\\.log$",
                       full.names = FALSE)
if (length(all_logs) == 0) {
  cli::cli_alert_info("No data logs found in logs/; nothing to summarize.")
  quit(status = 0)
}
prefix <- sub("_logfile_[0-9]{4}\\.log$", "", all_logs[1])
m <- regmatches(prefix, regexec("^(?:hoopR|wehoop)_([a-z]+)_data$", prefix, perl = TRUE))[[1]]
league <- if (length(m) == 2) toupper(m[2]) else "ESPN"

# Seasons: explicit range, else every season that has a log on disk.
if (!is.na(opt$s) && !is.na(opt$e)) {
  seasons <- opt$s:opt$e
} else {
  seasons <- sort(as.integer(sub(
    paste0("^", prefix, "_logfile_([0-9]{4})\\.log$"), "\\1", all_logs
  )))
}

# --- helpers ------------------------------------------------------------------
extract_updated <- function(lines) {
  # Legacy R producer: "Updated YYYY ESPN LEAGUE <ds> GitHub Release".
  m <- regmatches(
    lines,
    regexpr("Updated [0-9]{4} ESPN [A-Z]+ .+? GitHub Release", lines)
  )
  m <- m[nzchar(m)]
  ds <- sub("^Updated [0-9]{4} ESPN [A-Z]+ (.+?) GitHub Release$", "\\1", m)
  # Python producer (wnba_data_build): "uploaded <file> -> <tag> (asset N/M)".
  # The daily flow uses this now, so without it the summary reports 0 updated.
  p <- regmatches(
    lines,
    regexpr("uploaded \\S+ -> \\S+ \\(asset [0-9]", lines)
  )
  p <- p[nzchar(p)]
  ptags <- sub("^uploaded \\S+ -> (\\S+) \\(asset [0-9].*$", "\\1", p)
  sort(unique(c(ds, ptags)))
}

# cli warnings look like "! <ts>: skip <dataset> <season>/<id>: <reason>";
# R-level warnings surface as "Warning message(s):" + "cannot open URL ... 404".
extract_warnings <- function(lines) {
  w <- lines[
    grepl("^!\\s", lines) |
      grepl("cannot open URL.*40[0-9]", lines) |
      grepl("40[0-9] Not Found", lines) |
      grepl("no .* (rows|games|athletes) ", lines, ignore.case = TRUE)
  ]
  trimws(w)
}

extract_errors <- function(lines) {
  e <- lines[
    grepl("Execution halted", lines) |
      grepl("^Error[: ]", lines) |
      grepl("^✖|^x ", lines) |
      grepl("::error", lines)
  ]
  trimws(e)
}

# Group the 404/skip warnings into a one-line tally per dataset.
summarize_warnings <- function(w) {
  if (length(w) == 0) return(character(0))
  skip_ds <- regmatches(w, regexpr("skip [a-z_]+", w))
  skip_ds <- sub("^skip ", "", skip_ds)
  out <- character(0)
  if (length(skip_ds)) {
    tab <- table(skip_ds)
    out <- c(out, glue("{tab} game(s) skipped in {names(tab)} (missing/404 raw JSON)"))
  }
  n_404 <- sum(grepl("40[0-9]", w))
  other <- w[!grepl("^skip|cannot open URL|40[0-9]", w)]
  if (length(other)) out <- c(out, glue("{length(other)} other warning(s)"))
  out
}

# --- per-season report --------------------------------------------------------
cli::cli_h1("{league} ESPN Data — Run Summary ({min(seasons)}-{max(seasons)})")

tot_up <- 0L
tot_w <- 0L
tot_e <- 0L
md <- c(glue("## {league} ESPN Data — Run Summary ({min(seasons)}-{max(seasons)})"), "")

for (y in seasons) {
  logf <- glue("logs/{prefix}_logfile_{y}.log")
  if (!file.exists(logf)) {
    cli::cli_alert_info("Season {y}: no log on disk — skipped")
    next
  }
  lines <- readLines(logf, warn = FALSE)
  updated <- extract_updated(lines)
  warns <- extract_warnings(lines)
  errs <- extract_errors(lines)
  tot_up <- tot_up + length(updated)
  tot_w <- tot_w + length(warns)
  tot_e <- tot_e + length(errs)

  cli::cli_h2("Season {y}")
  if (length(updated)) {
    cli::cli_alert_success("{length(updated)} releases updated: {paste(updated, collapse = ', ')}")
  } else {
    cli::cli_alert_danger("No releases updated")
  }
  if (length(warns)) {
    cli::cli_alert_warning("{length(warns)} warning line(s):")
    cli::cli_ul(summarize_warnings(warns))
  }
  if (length(errs)) {
    cli::cli_alert_danger("{length(errs)} error line(s):")
    cli::cli_ul(utils::head(errs, 5))
  }
  if (!length(warns) && !length(errs)) cli::cli_alert_success("No warnings or errors")

  # markdown
  md <- c(md,
    glue("### Season {y}"),
    glue("- ✅ **{length(updated)} releases updated**: {ifelse(length(updated), paste(updated, collapse = ', '), '_none_')}"),
    if (length(warns)) c(glue("- ⚠️ **{length(warns)} warning line(s)**:"),
                         paste0("  - ", summarize_warnings(warns))) else glue("- ✅ no warnings"),
    if (length(errs)) c(glue("- ❌ **{length(errs)} error line(s)**:"),
                        paste0("  - `", utils::head(errs, 5), "`")) else glue("- ✅ no errors"),
    ""
  )
}

cli::cli_rule()
status_fn <- if (tot_e > 0) cli::cli_alert_danger else if (tot_w > 0) cli::cli_alert_warning else cli::cli_alert_success
status_fn("Totals across {length(seasons)} season(s): {tot_up} releases updated, {tot_w} warning line(s), {tot_e} error line(s)")

md <- c(md, "---",
  glue("**Totals across {length(seasons)} season(s):** {tot_up} releases updated · {tot_w} warning line(s) · {tot_e} error line(s)"))

gh <- Sys.getenv("GITHUB_STEP_SUMMARY")
if (nzchar(gh)) {
  tryCatch(writeLines(md, gh), error = function(e) {})
}
