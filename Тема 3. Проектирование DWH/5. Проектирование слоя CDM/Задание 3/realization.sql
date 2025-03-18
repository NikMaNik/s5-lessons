ALTER TABLE cdm.dm_settlement_report 
ADD CONSTRAINT dm_settlement_report_settlement_date_check
CHECK (settlement_date::date  >= '2022-01-01'::date AND settlement_date::date < '2500-01-01'::date);