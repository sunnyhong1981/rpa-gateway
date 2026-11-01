-- Review-only candidate. Not executed by tests or any deployment script.
-- Keep existing SELECT grants limited to authorized source views.
ALTER ROLE rpa_gateway CONNECTION LIMIT 20;
ALTER ROLE rpa_gateway_source CONNECTION LIMIT 12;
ALTER ROLE rpa_gateway_source SET default_transaction_read_only = on;
