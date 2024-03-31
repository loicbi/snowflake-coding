SELECT CURRENT_WAREHOUSE();

create schema if not exists DEMO_DB.ALF_SOURCE; -- will have source stage etc
create schema if not exists DEMO_DB.ALF_CURATED; -- data curation and de-duplication
create schema if not exists DEMO_DB.ALF_consumption; -- fact & dimension
create schema if not exists DEMO_DB.ALF_audit; -- to capture all audit records
create schema if not exists DEMO_DB.ALF_common; -- for file formats sequence object etc