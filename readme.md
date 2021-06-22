# Polish phone numbers parser
AWS Lambda function that parses 12 CSV files from https://numeracja.uke.gov.pl/en to a MySQL AWS RDS instance.

## Problems
The most difficult part was numbers, CSVs contains both single numbers and number ranges like '12203(0,2-9)', I needed to develop an additional 'parseNums' function that uses regex to break down such ranges into separate numbers. Besides that, column headers could be either 'Zakres number' or 'Numer' or it can be absent at all.

## Installation
Run
```bash
npm i
```
Zip all the files from the folder root and upload it to an AWS lambda function, set ENV varibales and start the function. 