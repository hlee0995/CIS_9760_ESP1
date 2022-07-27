# CIS_9760_ESP1
Title: Analyzing Millions of NYC Parking Violations with AWS EC2 &amp; Elastic Search

Projecct Description: First project in the CIS9760 Class with professor Ecem Basak. 
We utilized EC2, Elasticsearch, and Kibana to upload and analyze Open Parking and Camera Violations 
(https://dev.socrata.com/foundry/data.cityofnewyork.us/nc67-uf89)

Table of Content

project01/

+-- Dockerfile

+-- requirements.txt

+-- src/

+-- +-- main.py

+-- assets/

+-- +-- dashboard01.png 

+-- +-- dashboard02.png 

+-- +-- dashboard03.png 

+-- +-- dashboard04.png 

+-- README

Information about dataset
data contains with many columns but we onlt utlized followings
*Plate:
*State:
*License_type
*Summons_Number
*Issue_Date
*Violation
*fine_amount
*penalty_amount
*interest_amount
*reduction_amount
*payment_amount
*amount_due
*precinct
*county
*_id


How to run and install: 
1,Build Docker File with command line : docker build -t image01:1.0 .

2,Run docker file with command line :docker run -e DATASET_ID="nc67-uf89" -e APP_TOKEN="jnRNsxAqncIsd9aQ8edpY1O01" -e ES_HOST="https://search-cis9760-hyunjik-lee-tl3cq44xwz2qdfgeepsz5qz3ha.us-east-2.es.amazonaws.com" -e ES_USERNAME="hyunjik_lee" -e ES_PASSWORD="010265Lee@" -e INDEX_NAME="violation2" image1:1.0 --page_size=10000 --num_page=1200

Result: 
The result of uploading data on the Opensearch
![result](https://user-images.githubusercontent.com/82815882/161179243-e17183da-0e78-4d8d-b9da-ed1798b786d2.JPG)

1, Case numbers by years 
![dashboard01](https://user-images.githubusercontent.com/82815882/161179244-20c320f8-0582-4c36-86e9-d08eb2d77881.JPG)
2, Cases by amount_due
![dashboard02](https://user-images.githubusercontent.com/82815882/161179245-6aa881ee-a321-4c51-bf0f-b88c6547cf9a.JPG)
3, Total data points, Average fine amount, max fine amount
![dashboard03](https://user-images.githubusercontent.com/82815882/161179246-856be817-8d14-4931-bdec-44266c727bad.JPG)
4, Average Fine Amount by years
![dashboard04](https://user-images.githubusercontent.com/82815882/161179247-c76911a5-588b-4886-afbf-4198c780e1d1.JPG)



Updated on 03/27/2022
