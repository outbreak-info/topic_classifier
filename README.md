# Outbreak.info Resource library topic classifier
This script classifies publications in outbreak.info into broad topicCategories.
Works with python version 3.6 to 3.8.5

`refresh_annotations.py` - refreshes all the topicCategory annotations. Ie - Runs all needed scripts to get the topicCategory for all relevant records. Took ~1.5 hrs as of 2022.09.07 (~250K records). This should be run on a regular basis.

`update_annotations.py` - checks for new records and generates topicCategory annotations for them. DO NOT USE--the refresh function has been made more efficient, so this script no longer saves much time.

`update_training_data.py` - Pulls LitCovid categories and other training data generated using mapping from Clinical Trials (/src/clin_mapping.py), keyword searches (/data/keywords, /data/subtopics/keywords), and curator labeled data (/data/subtopics/curated_training_df.pickle). This should be run prior to updating a model.

`update_models.py` - re-trains the model(s) based on updated training data. This is slow and computationally expensive. Should be done only if there are changes in the training data which are expected to improve the model or if there are new categories.
Default test methods are included and the test results can be found at /results/in_depth_classifier_test.tsv

`/mutvariant_extraction.py` - has regex methods for extracting suspected mutations and variants, but the resulting content (/results/lineages.tsv, /results/mutations.tsv, /results/polymorphisms.tsv) have not been sufficiently tested for merging/inclusion into the resources API.

The topicCategories are generally not parsed from any original source so there is no risk of overwriting existing topicCategory data by accident. Hence, the results of the classifier may be added via BioThings SDK's default merging tools and using a dummy plugin for the generated data.

