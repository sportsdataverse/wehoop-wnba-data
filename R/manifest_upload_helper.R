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
