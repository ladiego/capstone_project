--A
SELECT 
	--user_id ,
	COUNT(user_id) AS total_transaksi 
FROM orders o
GROUP BY user_id 
ORDER BY total_transaksi DESC ;

--B
SELECT 
	SUM(total) AS Jumlah_order
FROM orders o

--C
SELECT 
	p.title, 
	COUNT(o.discount) AS total_order 
FROM orders o 
JOIN products p ON o.product_id = p.id 
WHERE o.discount IS NOT NULL 
GROUP BY title
ORDER BY total_order DESC 
LIMIT 10;

--D
WITH cte AS (
	SELECT 
		o.id,
		o.total,
		p.id,
		p.category
	FROM orders o 
	JOIN products p ON o.id = p.id
)
SELECT 
	category,
	SUM(total) AS total_order 
FROM cte
GROUP BY category
ORDER BY total_order DESC ;

--E
WITH cte AS (
	SELECT 
		o.id,
		o.total,
		p.id,
		p.category,
		p.title,
		p.rating
	FROM orders o 
	JOIN products p ON o.id = p.id
)
SELECT 
	title,
	SUM(total) AS total_order
FROM cte
WHERE rating >= 4
GROUP BY title
ORDER BY total_order DESC;

--F
WITH cte AS(
	SELECT 
		p.id,
		p.category,
		r.id,
		r.rating,
		r.created_at,
		r.body
	FROM products p 
	JOIN reviews r ON p.id = r.id
)
SELECT 
	body,
	created_at,
	rating,
	category
FROM cte
WHERE rating <=3 AND category = 'Doohickey'
ORDER BY created_at DESC ;

--G
SELECT 
	DISTINCT "source" 
FROM users u 

--H
WITH cte AS(
	SELECT  
		*
	FROM users u 
	WHERE email LIKE '%@gmail.com'
)

SELECT 
	COUNT(*) AS total_user_gmail
FROM Users
WHERE email LIKE '%@gmail.com';

--I
SELECT 
	id,
	title,
	price,
	created_at
FROM products p 
WHERE price >=30 AND price <=50
ORDER BY created_at DESC;

--J
CREATE VIEW User_after_1997 AS (
	SELECT 
		name,
		email,
		address,
		birth_date
	FROM users u 
	WHERE birth_date > '1997-12-31'
)
SELECT * FROM User_after_1997;

--K
WITH cte AS (
    SELECT 
        p.id,
        p.created_at,
        p.title,
        p.category,
        p.vendor,
        ROW_NUMBER() OVER (PARTITION BY p.title) AS row_num
    FROM Products p
)
SELECT id, created_at, title, category, vendor
FROM cte
WHERE title IN (
    SELECT title
    FROM cte
    GROUP BY title
    HAVING COUNT(*) > 1
);


