# CONFIG_SIGNALS Tab - Required Headers

## Copy this first row into your CONFIG_SIGNALS worksheet:

destination_key	destination_iata	destination_city	destination_country	primary_theme	jan_score	feb_score	mar_score	apr_score	may_score	jun_score	jul_score	aug_score	sep_score	oct_score	nov_score	dec_score	avg_temp_c	avg_rain_days	avg_sunshine_hrs	ski_season_start	ski_season_end	ski_snow_depth_cm	ski_snow_quality	ski_reliability_score	surf_season_start	surf_season_end	surf_water_temp_c	surf_wave_type	surf_consistency	surf_beginner_friendly

## Example Data Rows (after header):

KEF	KEF	Reykjavik	Iceland	winter_adventure	3	3	3	2	2	1	1	1	2	2	3	3	2	12	4						
BGO	BGO	Bergen	Norway	city_breaks	2	2	3	3	3	3	3	3	3	2	2	2	8	15	5						
TFS	TFS	Tenerife	Spain	winter_sun	3	3	3	3	2	1	0	0	1	2	3	3	21	3	7						
FAO	FAO	Faro	Portugal	winter_sun	3	3	3	3	2	2	1	1	2	2	3	3	18	6	6						
ESU	ESU	Essaouira	Morocco	surf	2	2	2	2	3	3	3	3	3	2	2	2	18	5	7			16	reef	consistent	TRUE
INN	INN	Innsbruck	Austria	snow	3	3	2	1	0	0	0	0	0	1	2	3	-2	8	5	DEC	APR	180	powder	0.9			

## Score Meanings:
- 3 = Peak season (best time to visit)
- 2 = Good timing
- 1 = Possible but not ideal
- 0 = Avoid (too hot/cold/wet/wrong season)

## Notes:
- destination_key and destination_iata should match (both use IATA code)
- Leave ski/surf columns empty for destinations that don't have those activities
- primary_theme options: winter_sun | surf | snow | city_breaks | shoulder
