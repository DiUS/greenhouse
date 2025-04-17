
# Download Activity Feed content for each Candidate and save

API_TOKEN=$1
echo Using token $API_TOKEN
cd $CACHE_DIR/candidates; echo In cache dir $(pwd)
for cf in $(ls | grep -v attachments | grep -v activity_feed); do
	candidate_id=$(basename $cf .json)
	echo -n '.'
	curl -s -S "https://harvest.greenhouse.io/v1/candidates/$candidate_id/activity_feed" \
	  -u "${API_TOKEN}:" > "${candidate_id}-activity_feed.json"
	[ $? -eq 0 ]  || echo "CURL error for candidate $candidate_id"
done
echo Done
