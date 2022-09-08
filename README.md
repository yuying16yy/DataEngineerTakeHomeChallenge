# Mistplay Data Engineer Take Home Challenge

This is a simple exercise for you to demonstrate your design and implementation skills
to solve a simple data engineering problem.  We don't want you to spend
a lot of time on it, but please treat it is something you would be checking in at 
work.

Fork the github repo [ here ](https://github.com/Mistplay/DataEngineerTakeHomeChallenge). Once you've completed the 
challenge, tar and gzip the repo and send via email back to Mistplay.

## Task Description

You are given a file called `data.json` which contains a list of records describing some sort of 
user activity, where each row has a `user_score` for the activity, and a `widget_list` containing information
about widgets involved in the activity.  There is also other information such as each user's `age_group`.

Your job is to transform and store the data, meeting the following requirements:

1. Output the number of rows in the original input file.
2. Dedupe the original data using the `id` and `created_at` columns (considered simultaneously).  In other words,
any rows with the same (`id`, `created_at`) tuple are considered identical (regardless of the value of other columns)
and all but 1 should be discarded.  (You can decide which ones to discard)
3. Output the number of rows removed.
4. Calculate the "rank" of the user in their `age_group` based on their `user_score` value.  Bigger `user_score`
is better. The user in the age group with the highest score is rank 1, second highest rank 2, and so on.
Store their rank within the dataset in a column called `age_group_rank`
5. Output the `id`, `email` and `age_group` for the top user (rank = 1) for each age group, in age group order (ascending)
6. Each record has an array of 0 or more dictionary/JSON objects in the `widget_list` column representing data about widgets
used in the activity.   Each element in this array looks like `{"name":"xxxx", "amount": 0.yyyy}`.  For each row,
'flatten' the `widget_list` resulting in one row for each element of the array in the `widget_list` column.  So if you
have a record that has two elements in the `widget_list` column (call them A and B), the result should be two rows that are 
identical to the original with a single dictionary/JSON element in the `widget_list` column, with values A and B respectively.
If a row has an empty array or null value in `widget_list` column, it remains as is with no additional rows.
7. Output the new total number of rows.
8. Add two more columns to the data set, called `widget_name` and `widget_amount`.  Then, for each row in the expanded data set,
extract the value of `name` and `amount` from the value in `widget_list` and put in the `widget_name` and `widget_amount` columns
respectively.  Figure out what to do if you have any rows with an empty or null `widget_list` value.
9. Anonymize the `email` column in place such that if needed, the anonymization could be reversed (if one had specific information).
10. Store the resulting table.  For now, write the resulting table to a `parquet` file on the local filesystem, but
make it relatively simple to change where the data is stored.
11. Create a new dataset that is an "inverted index" that has, for each country in the `location` column, a list of `id`s
that were seen in that country.  It should be two columns, called `location` and `ids`, where the `location` value
is the country code, and the `ids` value is an array if `id` values.
12. Store this inverted index table.  Like the main table, write it to a `parquet` file on local filesystem, with
same considerations for other storage locations as before.

Your code will be evaluated for correctness, scalability and maintainability.

## Guidelines

1. You are allowed to use any language and any libraries you wish. However, you should be able to justify your technical decisions.
2. Feel free to use any reference resources available to you (but please don't have another engineer help you directly.)
3. The challenge should not require more than a couple of hours to complete.
4. The challenge should be fun.
5. We'd love to hear any feedback you have about the challenge.

Thank you.
