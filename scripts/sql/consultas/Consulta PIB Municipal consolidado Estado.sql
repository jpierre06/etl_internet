SELECT 
	mi.id_Regiao	
	,mi.id_Estado
	,ri.sigla_Regiao
	,ei.sigla_Estado
	,ei.nome_Estado
	,spmi.ano_PIB
	,spmi.ano_Referencia
	,COUNT(spmi.id_Municipio) as quant_Municipios
	,ROUND(SUM(spmi.valor_PIB) / COUNT(spmi.id_Municipio) , 0) as valor_PIB_medio_Municipio
	,SUM(spmi.valor_PIB) as valor_PIB
	,SUM(spmi.populacao_Projetada) as populacao_Projetada
	,ROUND(SUM(spmi.valor_PIB) / SUM(spmi.populacao_Projetada) , 0)  as valor_PIB_percapita


FROM 
	IBGE.Regiao_IBGE as ri
		inner join
	IBGE.Estado_IBGE as ei on (ri.id_Regiao = ei.id_Regiao)
		inner join
	IBGE.Municipio_IBGE as mi on (mi.id_Estado = ei.id_Estado)
		inner join
	Stage.stg_PIB_Municipio_IBGE as spmi on (mi.id_Municipio = spmi.id_Municipio) 
	
	
WHERE TRUE
	AND mi.Data_Fim_Vigencia = '9999-12-31'	
	AND ei.Data_Fim_Vigencia = '9999-12-31'
	AND ri.Data_Fim_Vigencia = '9999-12-31'
	
GROUP BY
	mi.id_Regiao	
	,mi.id_Estado
	,ri.sigla_Regiao
	,ei.sigla_Estado
	,ei.nome_Estado
	,spmi.ano_PIB
	,spmi.ano_Referencia

ORDER BY
	spmi.ano_PIB ASC
	,valor_PIB_percapita DESC
	
;
