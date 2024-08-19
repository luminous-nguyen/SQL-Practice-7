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
       
-- Ex 6
WITH cte AS
         (
          SELECT transaction_id
                 merchant_id, 
                 credit_card_id, 
                 amount, 
                 transaction_timestamp as current_transaction,
                 LAG(transaction_timestamp) 
                 OVER(PARTITION BY merchant_id, credit_card_id, amount ORDER BY transaction_timestamp) AS previous_transaction
          FROM transactions
        )
        
SELECT COUNT(merchant_id) as payment_count
FROM cte
WHERE current_transaction-previous_transaction <= INTERVAL '10 minutes'
       
-- Ex 7
SELECT category,	
       product,
       total_spend
FROM(
      
      SELECT category,	
             product,
             SUM(spend) as total_spend,
             RANK()
             OVER(PARTITION BY category ORDER BY SUM(spend) DESC) as rank_cat
      FROM product_spend
      WHERE EXTRACT(YEAR FROM transaction_date) = 2022
      GROUP BY category, product
    ) cte
WHERE rank_cat IN (1,2)
       
-- Ex 8
WITH cte AS (
             SELECT 
                   artists.artist_name,
                   DENSE_RANK() 
                   OVER (ORDER BY COUNT(songs.song_id) DESC) AS artist_rank
             FROM artists
               INNER JOIN songs
               ON artists.artist_id = songs.artist_id
               INNER JOIN global_song_rank AS ranking
               ON songs.song_id = ranking.song_id
             WHERE ranking.rank <= 10
             GROUP BY artists.artist_name
           )

SELECT artist_name, artist_rank
FROM cte
WHERE artist_rank <= 5;





