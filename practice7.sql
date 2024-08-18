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





