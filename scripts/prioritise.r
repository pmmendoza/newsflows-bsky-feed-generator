stop(
  paste(
    "scripts/prioritise.r is retired.",
    "The /api/prioritize endpoint no longer writes integer post.priority values.",
    "Ranker output is now written through ranker_prod score tables."
  ),
  call. = FALSE
)
