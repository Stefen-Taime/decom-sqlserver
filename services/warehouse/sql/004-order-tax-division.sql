-- Preserve the source engine's six-place division result before the final ROUND.
-- Source golden case: quantity=1, price=0.01, tax=49.999 gives tax 0.000000.
-- Casting a PostgreSQL quotient directly rounds .0049999 to .005000 and changes the cent.
CREATE OR REPLACE FUNCTION app.order_line_amounts(quantity integer, unit_price numeric, tax_rate numeric)
 RETURNS TABLE(total_excluding_tax numeric,tax_amount numeric,total_including_tax numeric)
 LANGUAGE sql IMMUTABLE PARALLEL SAFE SECURITY INVOKER SET search_path=pg_catalog AS $$
 WITH inputs AS (SELECT unit_price::numeric(18,2) AS price,tax_rate::numeric(18,3) AS tax),
 base AS (SELECT (quantity::numeric(10,0)*price)::numeric(29,2) AS amount,tax FROM inputs),
 amounts AS (SELECT round(amount,2)::numeric(29,2) AS net,
  round(trunc((amount*tax)::numeric(38,5)*0.01,6)::numeric(38,6),2)::numeric(38,6) AS vat FROM base)
 SELECT net,vat,(net+vat)::numeric(38,6) FROM amounts
$$;
-- Multiplication by 0.01 is exact in PostgreSQL and avoids its division display-scale
-- rounding for large operands. The explicit truncation reproduces the tested SQL Server
-- 16 / compatibility-130 division behavior; it is not a generic SQL dialect translator.
