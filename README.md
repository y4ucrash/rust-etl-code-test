# Rust ETL Code Test

Fork this repo for this test. When you are done submit a PR against this repo.

Given the sample data provided, convert to csv in the format specified:

`name, billing_code, avg_rate` where `avg_rate` is the average of all `negotiated_rate` values for each record. Exclude records with an `avg_rate` greater than 30.

- Feel free to use any tools or libraries of your choice.
- The program should be as fast as possible.
- The program should accept inputs of unbounded size.
- The program should accept input from a file or STDIN.
- Output should be written to a file or STDOUT.
