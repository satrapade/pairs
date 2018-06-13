
--
-- Bucket P&L SQL template
--
-- this file needs to be processed with the "make_query" pre-processor 
-- to yield a valid SQL file
--
-- peprocessing timestamp: --R{timestamp()}--
-- 
-- parameters, values used
--
--  --R{no_text(  final_date<-as.character(Sys.Date(),format='%Y-%m-%d')   )}--
--
--  start_date               --R{if(!exists('start_date')) start_date<-"'2015-05-28'" else start_date}--
--  start_date_search        --R{if(!exists('start_date_search')) start_date_search<-"'2015-05-28'" else start_date_search}--
--  end_date                 --R{if(!exists('end_date')) end_date<-"'2018-05-01'" else end_date}--
--  end_date_search          --R{if(!exists('end_date_search')) end_date_search<-"'2018-05-01'" else end_date_search}--
--
--  --R{if(!exists('valuation_dates'))no_text(  vdr1<-make_date_range(start=start_date,end=end_date)     )}--
--  --R{if(!exists('valuation_dates'))no_text(  vdr2<-paste0("SELECT '",vdr1,"' AS date")                    )}--
--  --R{if(!exists('valuation_dates'))no_text(  valuation_dates<-paste(vdr2,collapse=" UNION ALL \n")        )}-- 
--
--  root_bucket_name         --R{if(!exists('root_bucket_name')) root_bucket_name<-"'EqyBucket'" else root_bucket_name}--
--  product_id               --R{if(!exists('product_id')) product_id<-'8' else product_id}--
--  position_data_source_id  --R{if(!exists('position_data_source_id')) position_data_source_id<-'2' else position_data_source_id}--
--  rates_data_source_id     --R{if(!exists('rates_data_source_id')) rates_data_source_id<-'2' else rates_data_source_id}--
--  pair_names               --R{if(!exists('pair_names')) pair_names<-"('DH_PAIR_20')" else nchar(pair_names)}--
--  excluded_flows           --R{if(!exists('excluded_flows')) excluded_flows<-"('Settlement')" else excluded_flows}--
--  output_table             --R{if(!exists('output_table')) output_table<-'tCHBFE' else output_table}--
--
-- 
-- --R{if(!exists('sql_tail'))no_text(    sql_tail<-paste0("SELECT * FROM ",output_table)     )}--
--

