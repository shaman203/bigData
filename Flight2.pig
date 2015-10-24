flights = load '/home/matyi/Egyetem/osz2/big_data_eszkozok/hazi/input/2008.csv' USING PigStorage(',') 
AS  (Year:chararray,
	Month:chararray,
	DayofMonth:chararray,
	DayOfWeek:chararray,
	DepTime:chararray,
	CRSDepTime:chararray,
	ArrTime:chararray,
	CRSArrTime:chararray,
	UniqueCarrier:chararray,
	FlightNum:chararray,
	TailNum:chararray,
	ActualElapsedTime:chararray,
	CRSElapsedTime:chararray,
	AirTime:chararray,
	ArrDelay:chararray,
	DepDelay:chararray,
	Origin:chararray,
	Dest:chararray,
	Distance:chararray,
	TaxiIn:chararray,
	TaxiOut:chararray,
	Cancelled:chararray,
	CancellationCode:chararray,
	Diverted:chararray,
	CarrierDelay:chararray,
	WeatherDelay:chararray,
	NASDelay:chararray,
	SecurityDelay:chararray,
	LateAircraftDelay:chararray);
completeRecords = FILTER flights BY ( Origin != 'NA');
originPorts = group completeRecords by Origin;
airportLiftoffs = foreach originPorts generate COUNT(completeRecords), group;
ordered = ORDER airportLiftoffs BY $0 DESC;
limited = LIMIT ordered 1;
projected = FOREACH limited GENERATE $1;
STORE projected into 'pig.out';
