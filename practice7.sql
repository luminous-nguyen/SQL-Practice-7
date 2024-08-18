-- Ex 1
SELECT 
       EXTRACT(YEAR FROM transaction_date) AS year,
       PRODUCT_ID,
       SPEND AS curr_year_spend,
       LAG(SPEND) OVER (
                        PARTITION BY PRODUCT_ID 
                        ORDER BY PRODUCT_ID, EXTRACT(YEAR FROM transaction_date)
                        ) AS prev_year_spend,
       ROUND(((spend/LAG(SPEND) OVER (
                        PARTITION BY PRODUCT_ID 
                        ORDER BY EXTRACT(YEAR FROM transaction_date)
                        )
              ) -1)*100,2)
                    
FROM user_transactions

-- Ex 2
SELECT
      DISTINCT
       card_name,
       FIRST_VALUE (issued_amount) 
       OVER(PARTITION BY card_name 
              ORDER BY issue_year,issue_month) AS issued_amount
      
FROM monthly_cards_issued
ORDER BY FIRST_VALUE (issued_amount) 
         OVER(PARTITION BY card_name 
              ORDER BY issue_year,issue_month) DESC
-- Ex 3
SELECT user_id,
       spend,
       transaction_date
FROM(
      SELECT user_id,
             spend,
             transaction_date,
             ROW_NUMBER() 
             OVER(PARTITION BY user_id ORDER BY transaction_date) as rank_transaction
      FROM transactions
     ) CTE 
WHERE rank_transaction = 3
-- Ex 4
SELECT transaction_date,
       user_id,
       COUNT(transaction_rank) AS purchase_count
FROM (
       SELECT product_id,	
              user_id,	
              spend,	
              transaction_date,
              DENSE_RANK() 
              OVER(PARTITION BY user_id ORDER BY transaction_date DESC) as transaction_rank
       FROM user_transactions
      ) CTE 
WHERE transaction_rank =1
GROUP BY transaction_date, user_id
-- Ex 5
SELECT user_id,
       tweet_date,
       ROUND(AVG(tweet_count) 
             OVER(PARTITION BY user_id ORDER BY tweet_date 
                  ROWS BETWEEN 2 PRECEDING AND CURRENT ROW),2) as rolling_avg_3d
FROM tweets





