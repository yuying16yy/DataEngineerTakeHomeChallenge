# Mistplay Data Engineer Take Home Challenge


## Task Description

You will be required to produce code to process and transform some sample data.
The sample data is in the file called `data.json`.
There are also some duplicate rows.

The produced code should be able to acheive the following
1. remove duplicates over the columns `id` and `created_at`
2. compute the rank of each user's `user_score` within each age group and output the rank in a new column called `sub_group_rank`
3. process the column `widget_list` by
    1. flattening the list items i.e. each item in the list is put into its own row
    2. extracting the values in the JSON elements into their own columns called `widget_name` and `widget_amount`
4. anonymize the column `email` and output the anonymized version in a new column `email_anon`.
This column `email_anon` should have the following properties.
    1. given an anonymized value the original value can be recovered
5. create a new table that is an inverted index that gives, for each country in `location,` which `id`s are located in that country
6. write the processed tables/data into separate `parquet` file(s).
Exactly how the files/tables are organized is not as important as having all the data present.

Your code will be evaluated for correctness, scalability and maintainability.

## Guidelines

1. You are allowed to use any language and any libraries you wish.
However, you should be able to justify your technical decisions.
Feel free to use any resources available to you.
2. Fork the github repo [ here ](https://github.com/Mistplay/DataEngineerTakeHomeChallenge). Once you've completed the challenge, push all code and other files to Github. Submit the link to your Github repo.
3. The challenge should not require more than a couple of hours to complete.
We don't want you to be spending too much time on it.
This being said, your code should be organized and well-designed within reason.
