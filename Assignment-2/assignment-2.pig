-- use CSV module
DEFINE CSVExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage; 

-- load csv file (file not included in repo because of it's size)
orderList = LOAD '/user/maria_dev/diplomacy/orders.csv' USING CSVExcelStorage() AS
(game_id:int,
unit_id:int,
unit_order:chararray,
location:chararray,
target:chararray,
target_dest:chararray,
success:int,
reason:int,
turn_num:int);

-- get rows that have Holland as a target, group by their locations, count the amount of times Holland was a target, and order the locations ascending
filteredByHolland = FILTER orderList BY target == 'Holland';
groupByLocationAndHolland = GROUP filteredByHolland BY (location, target);
countGroupedRows = FOREACH groupByLocationAndHolland GENERATE group, COUNT(filteredByHolland);
orderedLocationList = ORDER countGroupedRows BY $0 ASC;

-- print rows
DUMP orderedLocationList;