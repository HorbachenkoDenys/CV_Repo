Message1_Pie_bySpeakerCount:
	- Pie vizualization
	- metrics agregation by Count
	- buckets aggregation by Terms: speaker name
	- update every 2 minutes
	**/shows how many messages send every speaker/*

Message1_VertBar_bySpeakerCensoredCount:
	- Vertical Bar vizualization
	- metrics aggregation by Sum Bucket
	- Sum Bucket aggregation by Filters: word: censored
	- metric aggregation by Count
	- buckets aggregation by Terms: speaker
	- update every 2 minutes
	**/shows how many censored words say every Speaker/*

Message1_Table_byWordCount:
	- Horzintal Bar vizualization
	- metrics agregation by Count
	- metrics buckets aggregation by Terms: word
	- update every 2 minutes
	**/shows how many words was sayed/*
	
