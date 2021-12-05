# E: Get MPNS v8 analyses answers

This is not really a stage, but an area to answer some miscellaneous questions about the raw MPNS v8 tables (and not the
name mappings).

Some processing and filtering has to be done first at this stage, to be in line with the filtering done for the name
mappings, that is to filter out entries on unwanted `taxon_status`, and `quality_rating`.

There is only one result produced here at the moment. The results are output to
(data/analysis/mpns/mpns_v8/name_relationships/query)[data/analysis/mpns/mpns_v8/name_relationships/query].

## Questions:

### Which valid Plants in the MPNS v8 have the smallest number of relationships to Synonyms and Non-Scientific Names?

This question was used as an avenue to deciding the simplest and richest PubMed set of keywords (to retrieve abstracts) 
focused on as few plants as possible.

The intent was to remove as much complexity as possible out of the abstract retrieval process and later the
hand-annotation process.

Simple and rich, meaning the Plant could be associated with at least:
- one Synonym
- one Non-Scientific name of type Common
- one Non-Scientific name of type Pharmaceutical
- (and least importantly, but for completeness) one Non-Scientific name of type Sci-Cited Medicinal

And the results of this query were sorted by lowest number of associations within the above constraints first.

The results are at (data/analysis/mpns/mpns_v8/name_relationships/query)[data/analysis/mpns/mpns_v8/name_relationships/query]

In the end, this did not quite work, as the sets with lower numbers of name associations did not return enough results
from PubMed. I came to this conclusion by trial and error - plugging sets of names as keywords into the NER pipeline.

The solution I went with in the end was to find very common or popular herbs. For this I took inspiration from the book
Plants That Cure, by Elizabeth A Dauncey and Melanie-Jayne R Howes (2020).
