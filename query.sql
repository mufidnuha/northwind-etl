SELECT o.order_id, 
		o.customer_id,
		c2.company_name,
		o.order_date,
		p.product_id,
		p.product_name, 
		c.category_name as "category", 
		o.required_date,
		o.shipped_date,
		o.ship_via,
		o.ship_country,
		o.freight,
		od.unit_price, 
		od.quantity, 
		od.discount 
FROM orders o 
	JOIN customers c2 ON c2.customer_id = o.customer_id 
	JOIN order_details od ON o.order_id = od.order_id  
	JOIN products p ON od.product_id = p.product_id  
	JOIN categories c ON p.category_id = c.category_id 
WHERE DATE_PART('month', o.order_date) = DATE_PART('month', (SELECT current_date - INTERVAL '1 month' as previous_date))
		AND DATE_PART('year', o.order_date) = DATE_PART('year', (SELECT current_date - INTERVAL '1 month' as previous_date))