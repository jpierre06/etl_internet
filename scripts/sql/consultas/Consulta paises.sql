SELECT 
	p.*
	,scc.Country
	,scc.id_Fonte_dados
	,scc.Digit_Code_3
	,scc.Ano_Referencia
	
FROM 
	IBGE.Pais_IBGE as p
		inner join
	Stage.stg_Code_Country as scc on (p.ISO_Alpha_2 = scc.ISO_Alpha_2 and p.ISO_Alpha_3 = scc.ISO_Alpha_3) 
;


SELECT 
	id_Fonte_Dados 
	,COUNT(id_Pais) as count_country
FROM Internet_db.Pais
WHERE  Data_Fim_Vigencia = '9999-12-31'

GROUP BY
	id_Fonte_Dados
;
