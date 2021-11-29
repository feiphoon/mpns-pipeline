# C: Create Labels for annotation

This is to create labels for annotation with doccano in the NER pipeline. No code is required here as I created the labels my hand - they are at <data/reference/mpns_v8/annotation_labels.json>.

The required format is a JSON file (example from the doccano server walkthrough):

```json
[
    {
        "text": "Dog",
        "suffix_key": "d",
        "background_color": "#FF0000",
        "text_color": "#ffffff"
    },
    {
        "text": "Cat",
        "suffix_key": "c",
        "background_color": "#FF0000",
        "text_color": "#ffffff"
    }
]
```

- `text`: the label name
- `suffix_key`: the desired keyboard shortcut
- `background_color`: a hex code for the label background colour
- `text_color`: a hex code for the label text colour

## The labels we need:

- `scientific`: This should cover plant full scientific names from plants, full scientific names from synonyms, and the class of names from the non-scientific names dataset labelled as "sci_cited_medicinal"
- `common`: This is the class of names from the non-scientific names dataset labelled as "common"
- `pharmaceutical`: This isthe class of names from the non-scientific names dataset labelled as "pharmaceutical"
