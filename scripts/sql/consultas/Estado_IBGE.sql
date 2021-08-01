CREATE TABLE IBGE.Estado_IBGE (
	idEstado INT NOT NULL,
	siglaEstado varchar(2) NOT NULL,
	nomeEstado varchar(50) NOT NULL,
	idRegiao INT NOT NULL,
	CONSTRAINT Estado_IBGE_PK PRIMARY KEY (idEstado)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8
COLLATE=utf8_general_ci;
