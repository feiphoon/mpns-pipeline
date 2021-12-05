# D: MPNS v8 name relationships analysis

This step is to analyse the name mappings in MPNS v8 to understand what distribution of name types we had **before**
coaxing the data into name mappings for the annotated abstract synthesis pipeline (the NER pipeline).

Some processing and filtering has to be done first at this stage, to be in line with the filtering done for the name
mappings, that is to filter out entries on unwanted `taxon_status`, and `quality_rating`.