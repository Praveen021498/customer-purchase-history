# customer-purchase-history

Analyzed customer purchasing behavior by ranking their orders based on the
purchase date.

Data Ingestion: Loaded customer purchase history from a structured data source from amazon s3.

Data Cleaning:
  Removed duplicate records and filter invalid entries (e.g., negative order
  amounts).

Data Transformation:
  Converted date fields to a readable or understandable format.
  Used windowing operation to rank purchases by date per customer
  (row_number() or rank() window functions).

Data Enrichment:
  Added cumulative spending per customer using a running total calculation with the
  window function (sum() over a window).

Data Storage: 
  Write the transformed data into a Delta Lake table, partitioned by
  customer_id for better query performance.
