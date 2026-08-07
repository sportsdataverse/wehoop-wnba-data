# Shared helper -- upload the per-dataset manifest CSV to its release tag.
# See wehoop-wbb-data/R/manifest_upload_helper.R for full docs.

upload_wnba_manifest <- function(manifest_path,
                                 release_tag,
                                 file_name,
                                 sportsdataverse_type = "manifest",
                                 pkg_function         = NA_character_) {
  if (!file.exists(manifest_path)) {
    return(invisible(NULL))
  }
  manifest_df <- readr::read_csv(manifest_path, show_col_types = FALSE)
  if (nrow(manifest_df) == 0) return(invisible(NULL))
  manifest_df <- manifest_df %>%
    dplyr::distinct(.data$season, .keep_all = TRUE) %>%
    dplyr::arrange(.data$season)

  save_manifest <- purrr::insistently(
    sportsdataversedata::sportsdataverse_save,
    rate = purrr::rate_backoff(pause_base = 1, pause_min = 1, max_times = 5),
    quiet = FALSE
  )
  save_manifest(
    data_frame           = manifest_df,
    file_name            = file_name,
    sportsdataverse_type = sportsdataverse_type,
    release_tag          = release_tag,
    pkg_function         = pkg_function,
    file_types           = c("csv"),
    .token               = Sys.getenv("GITHUB_PAT")
  )
  invisible(manifest_df)
}


# Shared helper -- upsert ONE row per season into a manifest CSV.
#
# See wehoop-wbb-data/R/manifest_upload_helper.R for full docs.
#
# Args:
#   manifest_path: CSV to upsert into; created if absent.
#   manifest_row:  one-row data frame/tibble for `season`.
#   season:        the season this row describes. Passed EXPLICITLY so the
#                  caller's variable name cannot be assumed.
upsert_manifest_row <- function(manifest_path, manifest_row, season_value) {
  # The argument is `season_value`, NOT `season`, and the mask is computed
  # OUTSIDE the subset. Inside data.table's `[`, a bare `season` resolves to
  # the COLUMN, so `prior[prior$season != season]` means
  # `prior$season != prior$season` -- always FALSE, wiping every prior season
  # and leaving one row behind. That silently destroyed 23 seasons of manifest
  # history in testing before it was caught.
  season_value <- as.integer(season_value)
  if (file.exists(manifest_path)) {
    prior <- data.table::fread(manifest_path)
    keep <- prior$season != season_value
    prior <- prior[keep]
    manifest_row <- data.table::rbindlist(
      list(prior, manifest_row), use.names = TRUE, fill = TRUE
    )
  }
  manifest_row <- data.table::as.data.table(manifest_row)
  data.table::setorderv(manifest_row, "season")
  data.table::fwrite(manifest_row, manifest_path)
  invisible(manifest_path)
}
