# sturdy-meme
Repository to demonstrate sample data engineering assignment

# Data Engineering on retail dataset
In this project, we will perform operations on raw reatil datasets to acheive results for analytic queries as part of assignment

- [Background](#background)
- [Assumptions](#assumptions)
- [Application Flow](#application-flow)
- [Data model diagram](#data-model-diagram)
- [How to execute jobs](#how-to-execute-jobs)
- [Analytic queries with results](#analytic_queries-with-results)

***

## Background

We have three data sets as follows - 
- [customers.csv](data-eng-project%2Fsrc%2Fmain%2Fresources%2Fbronze%2Fcustomer%2Fcustomers.csv)
- [products.csv](data-eng-project%2Fsrc%2Fmain%2Fresources%2Fbronze%2Fproducts%2Fproducts.csv)
- [orders.csv](data-eng-project%2Fsrc%2Fmain%2Fresources%2Fbronze%2Forders%2Forders.csv) 

Before proceeding with data lets visit expected ingestion frequency for them. 
This will help us to build system/flow accordingly

|   Name   | Ingested frequency | Format |
|:--------:|:------------------:|:------:|
| customer |       daily        |  csv   |
| products |       daily        |  csv   |
|  orders  |       hourly       |  csv   |


Based on above information we need to provide result of following queries
- Top 10 countries with the most number of customers.
- Revenue distribution by country.
- Relationship between average unit price of products and their sales volume.
- Top 3 products with the maximum unit price drop in the last month.

---

## Assumptions

Before proceeding with actual development of scripts lets consider few assumptions which I have taken by looking at the given data 

<ol>
  <li>Assumption for validating Customer data 
      <ol>
        <li>Remove records whose CustomerID or Country value is null</li>
        <li>For duplicate records, pick the last record as per country value ordering
        e.g. 
                  <table>
                      <tr>
                          <td>CustomerId</td>
                          <td>Country</td>                  
                      </tr>
                      <tr>
                          <td>12394</td>
                          <td>Belgium</td>                      
                      </tr>
                     <tr>
                          <td>12394</td>
                          <td>Denamrk</td>                      
                      </tr>
                  </table>
        here, will select second record
                  <table>
                      <tr>
                          <td>CustomerId</td>
                          <td>Country</td>                  
                      </tr>
                     <tr>
                          <td>12394</td>
                          <td>Denamrk</td>                      
                      </tr>
                  </table>
        </li>
      </ol>
  </li>
  <li>Assumption for validating Products data
      <ol>
        <li>Remove records whose Description or UnitPrice or StockCode value is null</li>
        <li>Remove records whose UnitPrice value is less than or equal to 0</li>
        <li>Remove all records whose description is not present in uppercase </li>
        <li>Remove all records whose StockCode value is not present in either of below formats
            <ul>
              <li>6 digit e.g. 84748</li>
              <li>6 digit + code value e.g. 10124A</li>
            </ul>
        </li>
        <li>In case of duplicate records -
            <ul>
              <li>For duplicate StockCode, choose record whose description is not null and present in uppercase e.g. 
                  <table>
                    <tr>
                        <td>StockCode</td>
                        <td>Description</td>
                        <td>Unit Price</td>
                    </tr>
                    <tr>
                        <td>85092</td>
                        <td>CANDY SPOT TEA COSY</td>
                        <td>1.56</td>
                    </tr>
                    <tr>
                        <td>85092</td>
                        <td></td>
                        <td>1.52</td>
                    </tr>
                  </table>
                Here, will select first record
                    <table>
                        <tr>
                            <td>StockCode</td>
                            <td>Description</td>
                            <td>Unit Price</td>
                        </tr>
                        <tr>
                            <td>85092</td>
                            <td>CANDY SPOT TEA COSY</td>
                            <td>1.56</td>
                        </tr>
                    </table>
              </li> 
              <li>For duplicate description, choose record whose StockCode biggest among them e.g.
                    <table>
                          <tr>
                              <td>StockCode</td>
                              <td>Description</td>
                              <td>Unit Price</td>
                          </tr>
                          <tr>
                              <td>79190A</td>
                              <td>RETRO PLASTIC 70'S TRAY</td>
                              <td>1.56</td>
                          </tr>
                          <tr>
                              <td>79192A</td>
                              <td>RETRO PLASTIC 70'S TRAY</td>
                              <td>1.52</td>
                          </tr>
                    </table>
                  here, will select second record
                  <table>
                          <tr>
                              <td>StockCode</td>
                              <td>Description</td>
                              <td>Unit Price</td>
                          </tr>
                          <tr>
                              <td>79192A</td>
                              <td>RETRO PLASTIC 70'S TRAY</td>
                              <td>1.52</td>
                          </tr>
                    </table>
              </li>
              <li>For duplicate stockcodes, select records whose description is smallest among them e.g.
                    <table>
                          <tr>
                              <td>StockCode</td>
                              <td>Description</td>
                              <td>Unit Price</td>
                          </tr>
                          <tr>
                              <td>23236</td>
                              <td>DOILEY BISCUIT TIN</td>
                              <td>1.05</td>
                          </tr>
                          <tr>
                              <td>23236</td>
                              <td>DOILEY STORAGE TIN</td>
                              <td>2.31</td>
                          </tr>
                    </table>
                  here, will select second record
                  <table>
                          <tr>
                              <td>StockCode</td>
                              <td>Description</td>
                              <td>Unit Price</td>
                          </tr>
                          <tr>
                              <td>23236</td>
                              <td>DOILEY BISCUIT TIN</td>
                              <td>1.05</td>
                          </tr>
                    </table>
              </li>
            </ul>
        </li>
      </ol>
  </li>
  <li>Assumption for validating Orders data
    <ol>
        <li>Remove records whose CustomerID is null</li>
        <li>Remove records whose quantity is invalid</li>
        <li>Remove records whose InvoiceNo is duplicated across CustomerID</li>
    </ol>
  </li>

</ol>

---

## Application Flow

- All files will be kept under 'bronze' directory. This will act as staging directory.
- In first stage, will do filtering, cleansing, validation as per assumption and enrichment of additional attributes as per requirement. 
- Then, processed data will get store intermediately at 'silver' directory.
- intermediate data from 'silver' directory act as input to final jobs where aggregation and population of other metadata attributes will take place.
- Finally, all processed data will be put under 'gold' directory. This layer will act as final data model layer.

|   Name   | bronze location and format | silver location and format |    gold location and format    |
|:--------:|:--------------------------:|:--------------------------:|:------------------------------:|
| customer |  '/bronze/customer/*.csv'  |  '/silver/customer/*.csv'  | '/gold/customer_dim/*.parquet' |
| products |  '/bronze/products/*.csv'  |  '/silver/products/*.csv'  | '/gold/products_dim/*.parquet' |
|  orders  |   '/bronze/orders/*.csv'   |   '/silver/orders/*.csv'   | '/gold/orders_fact/*.parquet'  |


Layer wise attribute details 
- customer
  
  - bronze layer
    - customerid
    - country
  
  - silver layer
    - customerid
    - country
    - country_code
    - region
  
  - gold layer
    - customerid 
    - country 
    - country_code 
    - region 
    - insert_date 
    - updated_date

- products

    - bronze layer
        - stockcode 
        - description 
        - unitprice

    - silver layer
        - stockcode
        - description
        - unitprice

    - gold layer
        - stockcode 
        - product_code e.g. if stock_code is '123A' then product code is '123' 
        - product_category 
        - description 
        - unitprice 
        - eff_start_date 
        - eff_end_date

- orders

    - same across all layers
        - invoiceno 
        - stockcode 
        - quantity 
        - InvoiceDate 
        - customerid

---

### Data model diagram

![img_3.png](img_3.png)

---

## How to execute jobs

- To get required data in final layer we have some dependency to process data
- I'm processing dimension data first. As it will be used for validation step in loading of fact data
- Lets look out directory structure 
  - ![img.png](img.png)
  - As mentioned above we need to process customer data first
    - There are two jobs which needs to execute  
      - `LoadCustomerDataToSilverSpark (package - main.spark.bronze_to_silver)`
      - `LoadCustomerDataToGoldSpark (package - main.spark.silver_to_gold)`
    - Note - as we are executing locally, you can use any IDE to check out code and execute them
    - We can check if data is populated in directory 'gold/customer_dim'
      - ![img_1.png](img_1.png)
  - Now similarly we need to execute jobs to process products data
    - `LoadProductsDataToSilverSpark (package - main.spark.bronze_to_silver)`
    - `LoadProductsDataToGoldSpark (package - main.spark.silver_to_gold)`
  - Then finally execute jobs to process orders data
    - `LoadOrdersDataToSilverSpark (package - main.spark.bronze_to_silver)`
    - `LoadOrdersDataToGoldSpark (package - main.spark.silver_to_gold)`

---

## Analytic_queries with results

- Once data is populated in final layer i.e. 'gold'. 
  We can execute Task jobs to provide results for analytic queries mentioned above [here](#background)

    ## Task 1 - Top 10 countries with the most number of customers
        Execute `Task1` job to see results (package - spark.analytics)
        Result
    ![img_4.png](img_4.png)

    ### similarly we can execute remaining Task jobs from package - spark.analytics to get remaining results

---