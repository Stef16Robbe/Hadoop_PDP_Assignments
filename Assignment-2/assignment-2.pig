DEFINE CSVExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage; 

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

filteredByHolland = FILTER orderList BY target == 'Holland';
groupByLocationAndHolland = GROUP filteredByHolland BY (location, target);
countGroupedRows = FOREACH groupByLocationAndHolland GENERATE group, COUNT(filteredByHolland);
orderedLocationList = ORDER countGroupedRows BY $0 ASC;

DUMP orderedLocationList;