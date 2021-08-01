CREATE TABLE IBGE.Regiao_IBGE (
  `idRegiao` int NOT NULL,
  `siglaRegiao` varchar(100) NOT NULL,
  `nomeRegiao` varchar(100) NOT NULL
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8
COLLATE=utf8_general_ci;


ALTER TABLE IBGE.Regiao_IBGE ADD CONSTRAINT Regiao_IBGE_PK PRIMARY KEY (idRegiao);
