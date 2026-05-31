stop(
  paste(
    "scripts/priorities.r is retired.",
    "The /api/priorities endpoint no longer exposes integer-priority views.",
    "Use canonical feed, engagement, or study endpoints; add a score-specific",
    "read endpoint only after it is approved and documented."
  ),
  call. = FALSE
)
