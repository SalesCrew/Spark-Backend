UPDATE "markets"
SET
  "standard_market_number" = NULLIF(BTRIM("standard_market_number"), ''),
  "coke_master_number" = NULLIF(BTRIM("coke_master_number"), ''),
  "flex_number" = NULLIF(BTRIM("flex_number"), '');
