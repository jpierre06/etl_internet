SELECT 
	mi.id_Regiao	
	,mi.id_Estado
	,mi.id_Municipio
	,ri.sigla_Regiao
	,ei.sigla_Estado
	,ei.nome_Estado
	,mi.nome_Municipio
	,spmi.ano_PIB
	,spmi.ano_Referencia
	,spmi.valor_PIB
	,spmi.valor_PIB_percapita
	,ROUND(spmi.valor_PIB_percapita / 12 , 0) as valor_PIB_percapita_medio_Mes
	,spmi.populacao_Projetada


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
	AND ei.sigla_Estado = 'SE'
	
ORDER BY
	spmi.ano_PIB ASC
	,valor_PIB_percapita DESC
	
;
