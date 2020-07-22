# tower-backend

/cache_holdings_titans - Issues SQS for caching titan holdings via S3

bot-api.us-east-1.elasticbeanstalk.com/cache_holdings_titans

## 1

daily_2A_PST => fetchHoldings_Titans
Loop over all billionaires, use cik to fetch holdings from INTRINIO/zacks and save to S3
Assign ticker ownership for company page
Saves S3 data_url

    GET request to /cache_holdings_titans => SQS

    QUEUE_NAME : AWS_SQS_URL_BILLIONAIRE_HOLDINGS => consumer_1

## 2

daily_3A_PST => cache_performances

    Loop over all billionaires, use cik to fetch file S3 and evaluate

        Top 10 holdins
        Portfolio composition
        Performance

    GET request to /cache_performances_titans => SQS

    QUEUE_NAME : AWS_SQS_URL_BILLIONAIRE_PERFORMANCES => consumer_2

    =>

1. Two ways calculate perfomances (A and B)
2. Calculate way B is difficult, we need to evaluate

   A) GLOBAL : BKSHIRE 7/20 marketcap - 6/20 marketcap percentage = performance

   B) NAV/COMPOSITE : BERKSHIRE PORTFOLIO perfomance 7/20 - 6/20 % delta composite
   Example:

   TSLA 7/20 - 7/19
   INTC 7/20 - 7/18
   DEL 7/20 - 7/17

   Loop over last batch holdings and process all companies.

   publish_ProcessCompanyLookup - fetch useful metadata (currently sector for portfolio concentration on titan summary interface)

   QUEUE_NAME : AWS_SQS_URL_COMPANY_LOOKUP => consumer_3

   publish_ProcessSecurityPrices

   QUEUE_NAME : AWS_SQS_URL_SECURITY_PRICES => consumer_4
