/*
-- Stage.stg_Capital_Country definition

CREATE TABLE `Capital_Country` (
  `id_Fonte_Dados` int DEFAULT NULL,
  `id_Country` int DEFAULT NULL,
  `Country` varchar(200),
  `Capital` varchar(200),
  `Ano_Referencia` int DEFAULT NULL,
  `Data_Extracao` datetime DEFAULT NULL,
  `Data_Inicio_Vigencia` date DEFAULT NULL,
  `Data_Fim_Vigencia` date DEFAULT NULL
) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4;
*/

/*

ALTER TABLE IBGE.Regiao_Mundo_IBGE DROP PRIMARY KEY;
ALTER TABLE IBGE.Regiao_Mundo_IBGE ADD CONSTRAINT Regiao_IBGE_PK PRIMARY KEY (id_Regiao,Data_Fim_Vigencia);


*/



/*
CALL Stage.Versiona_stg_Pais();


DELETE FROM Internet_db.Country
WHERE 
	id_Fonte_Dados = 6
;



UPDATE Internet_db.Country as p
SET 
	p.Data_Inicio_Vigencia = DATE('2021-07-07')
WHERE 
	id_Fonte_Dados = 6;



UPDATE IBGE.Subregiao_Mundo_IBGE as p
SET 
	p.Data_Inicio_Vigencia = DATE(p.Data_Extracao)
	,p.Data_Fim_Vigencia = DATE('9999-12-31')
	,p.id_Fonte_Dados = 9
WHERE TRUE;



UPDATE IBGE.Pais_IBGE as p
SET 
	#p.Data_Inicio_Vigencia = DATE(p.Data_Extracao)
	#,p.Data_Fim_Vigencia = DATE('9999-12-31')
	p.id_Fonte_Dados = 9
WHERE TRUE
	AND p.id_Fonte_Dados IS NULL
;



UPDATE IBGE.Regiao_IBGE as p
SET 
	p.Data_Inicio_Vigencia = DATE(p.Data_Extracao)
	,p.Data_Fim_Vigencia = DATE('9999-12-31')
	,p.id_Fonte_Dados = 10
WHERE TRUE;





UPDATE IBGE.Estado_IBGE as p
SET 
	#p.Data_Inicio_Vigencia = DATE(p.Data_Extracao)
	#,p.Data_Fim_Vigencia = DATE('9999-12-31')
	p.id_Fonte_Dados = 11
WHERE TRUE;


UPDATE IBGE.Mesoregiao_IBGE as p
SET 
	p.Data_Inicio_Vigencia = DATE(p.Data_Extracao)
	,p.Data_Fim_Vigencia = DATE('9999-12-31')
	,p.id_Fonte_Dados = 12
WHERE TRUE;


UPDATE IBGE.Sub_Distrito_IBGE as p
SET 
	p.Data_Inicio_Vigencia = DATE(p.Data_Extracao)
	,p.Data_Fim_Vigencia = DATE('9999-12-31')
	,p.id_Fonte_Dados = 18
WHERE TRUE;

*/