CREATE TABLE IBGE.Mesoregiao_IBGE (
	id_Mesoregiao INT NOT NULL,
	id_Estado INT NOT NULL,
	id_Regiao INT NOT NULL,
	nome_Mesoregiao varchar(200) NOT NULL,
	Data_Extracao DATETIME NOT NULL,
	CONSTRAINT Mesoregiao_IBGE_PK PRIMARY KEY (id_Mesoregiao)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8
COLLATE=utf8_general_ci
COMMENT='Extração site IBGE

https://servicodados.ibge.gov.br/api/docs/localidades?versao=1#api-Mesorregioes-mesorregioesGet';

CREATE INDEX Mesoregiao_IBGE_id_Mesoregiao_IDX USING BTREE ON IBGE.Mesoregiao_IBGE (id_Mesoregiao);
