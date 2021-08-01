DELIMITER //

CREATE PROCEDURE Stage.Versiona_stg_Fonte_Dados_Internet()

BEGIN

/*
Criando em 06.07.2021, Jean Pierre

 */

DECLARE REG_VERSIONA int default 0; 
DECLARE maxID int default 0; 

CREATE TEMPORARY TABLE TEMP_VERSIONA AS  (

	SELECT  
		TEMP.Nome_Fonte_Dados
		,TEMP.Site_Fonte_Dados
		,TEMP.Nome_Site

FROM 
	Stage.stg_Fonte_Dados as TEMP

WHERE
	NOT EXISTS (SELECT 1 
				FROM 
					Internet_db.Fonte_Dados_Internet as p 
				WHERE 
					TEMP.Nome_Fonte_Dados = p.Nome_Fonte_Dados 
				) 
) ;
# END TEMP_VERSIONA 



  
SET REG_VERSIONA = (SELECT COUNT(*) FROM TEMP_VERSIONA) ;
  
IF REG_VERSIONA > 0 THEN


	INSERT Internet_db.Fonte_Dados_Internet (
		Nome_Fonte_Dados
		,Site_Fonte_Dados
		,Nome_Site
	)         
	SELECT  
		Nome_Fonte_Dados
		,Site_Fonte_Dados
		,Nome_Site

	FROM TEMP_VERSIONA 
	; # INSERT Internet_db.Fonte_Dados_Internet



END IF; # IF REG_VERSIONA > 0 THEN
  

END//


DELIMITER ; 