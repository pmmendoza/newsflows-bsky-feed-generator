library(httr2)
readRenviron(".env")
feedgen_read_key <- function() {
  key <- Sys.getenv("FEEDGEN_READ_API_KEY", "")
  if (key == "") key <- Sys.getenv("FEEDGEN_MONITOR_API_KEY", "")
  if (key == "") key <- Sys.getenv("FEEDGEN_RANKER_API_KEY", "")
  key
}
# for https servers use 
# subscribers <- request(paste0("https://", Sys.getenv("FEEDGEN_HOSTNAME"))) |> 
subscribers <- request(paste0("http://", Sys.getenv("FEEDGEN_HOSTNAME"), ":3020")) |> 
  req_url_path("/api/subscribers") |> 
  req_headers("api-key" = feedgen_read_key(), .redact = "api-key") |> 
  req_perform() |> 
  resp_body_json()

subscribers_df <- subscribers |> 
  purrr::pluck("subscribers") |> 
  dplyr::bind_rows()
