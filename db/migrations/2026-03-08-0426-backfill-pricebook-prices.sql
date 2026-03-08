-- Backfill pricebook_price_cad and pricebook_price_usd for parts that were inserted
-- before these columns were added. Pricebook price = 1.1x MSRP at time of creation.
-- This is a one-time backfill; the upload job now sets these on INSERT going forward.
UPDATE public.parts
SET
    pricebook_price_cad = ROUND(msrp_cad * 1.1, 2),
    pricebook_price_usd = ROUND(msrp_usd * 1.1, 2)
WHERE pricebook_price_cad IS NULL
   OR pricebook_price_usd IS NULL;
