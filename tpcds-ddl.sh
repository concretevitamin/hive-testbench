#####
#####
##### Assumed data is already generated as text files.
##### Required vars: `S3KEY`, `S3SECRET`, and `S3BUCKET`. Supported: `FORMAT`.
#####
#####

SCALE=1000
DIR="s3n://${S3KEY}:${S3SECRET}@${S3BUCKET}/tpcds/sf${SCALE}"

DIMS="date_dim time_dim item customer customer_demographics household_demographics customer_address store promotion warehouse ship_mode reason income_band call_center web_page catalog_page web_site"
FACTS="store_sales store_returns web_sales web_returns catalog_sales catalog_returns inventory"

function runcommand {
	if [ "X$DEBUG_SCRIPT" != "X" ]; then
		$1
	else
		$1 2>/dev/null
	fi
}

# Create the text/flat tables as external tables. These will be later be converted to ORCFile.
echo "Loading text data into external tables."
runcommand "hive -i settings/load-flat.sql -f ddl-tpcds/text/alltables.sql -d DB=tpcds_text_${SCALE} -d LOCATION=${DIR}"

# Create the partitioned and bucketed tables.
if [ "X$FORMAT" = "X" ]; then
	FORMAT=orc
fi
i=1
total=24
DATABASE=tpcds_bin_partitioned_${FORMAT}_${SCALE}
for t in ${FACTS}
do
	echo "Optimizing table $t ($i/$total)."
	COMMAND="hive -i settings/load-partitioned.sql -f ddl-tpcds/bin_partitioned/${t}.sql \
	    -d DB=tpcds_bin_partitioned_${FORMAT}_${SCALE} \
	    -d SOURCE=tpcds_text_${SCALE} -d BUCKETS=${BUCKETS} \
	    -d RETURN_BUCKETS=${RETURN_BUCKETS} -d FILE=${FORMAT}"
	runcommand "$COMMAND"
	if [ $? -ne 0 ]; then
		echo "Command failed, try 'export DEBUG_SCRIPT=ON' and re-running"
		exit 1
	fi
	i=`expr $i + 1`
done

# After the above block, should be able to find new files under warehouse.

# Populate the smaller tables.
for t in ${DIMS}
do
	echo "Optimizing table $t ($i/$total)."
	COMMAND="hive -i settings/load-partitioned.sql -f ddl-tpcds/bin_partitioned/${t}.sql \
	    -d DB=tpcds_bin_partitioned_${FORMAT}_${SCALE} -d SOURCE=tpcds_text_${SCALE} \
	    -d FILE=${FORMAT}"
	runcommand "$COMMAND"
	if [ $? -ne 0 ]; then
		echo "Command failed, try 'export DEBUG_SCRIPT=ON' and re-running"
		exit 1
	fi
	i=`expr $i + 1`
done

echo "Data loaded into database ${DATABASE}."
