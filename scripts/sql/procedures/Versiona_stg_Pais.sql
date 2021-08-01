DELIMITER //

CREATE PROCEDURE Stage.Versiona_stg_Pais()

BEGIN

/*
Criando em 05.07.2021, Jean Pierre

 */

DECLARE REG_VERSIONA int default 0; 
DECLARE maxID int default 0; 

CREATE TEMPORARY TABLE TEMP_VERSIONA AS  (

	SELECT  
		temp.id_Fonte_Dados
		,temp.Nome_Pais
		,temp.Data_Inicio_Vigencia
		,temp.Data_Fim_Vigencia

FROM 
	Stage.stg_Pais as TEMP

        WHERE NOT EXISTS (SELECT 1 
							FROM 
								Internet_db.Pais as p 
                            WHERE 
								DATE(p.Data_Fim_Vigencia)  = '9999-12-31'
								AND temp.id_Fonte_Dados = p.id_Fonte_Dados 
								AND temp.Nome_Pais = p.Nome_Pais 
                            ) 
		ORDER BY
			temp.id_Fonte_Dados
) ;
# END TEMP_VERSIONA 



  
  SET REG_VERSIONA = (SELECT COUNT(*) FROM TEMP_VERSIONA) ;
  
  IF REG_VERSIONA > 0 THEN

     UPDATE Internet_db.Pais as p 
     SET p.Data_Fim_Vigencia  = CAST(CURRENT_DATE() as DATE) 
     WHERE 
		p.Data_Fim_Vigencia  = CAST('9999-12-31' as DATE) 
		AND EXISTS (SELECT 1 
					FROM TEMP_VERSIONA AS TEMP
					WHERE 
						TEMP.id_Fonte_Dados= p.id_Fonte_Dados
						AND TEMP.Nome_Pais = FAT.Nome_Pais
					)
		; # UPDATE Internet_db.Pais as p
      
     SET maxID = (SELECT COALESCE(MAX(id_Pais), 0)  FROM Internet_db.Pais)  ;
     
	CREATE TEMPORARY TABLE VERSIONA_TRATADA AS  ( 
    
            SELECT  
					ROW_NUMBER() OVER() + maxID as id_Pais
					,TEMP.id_Fonte_Dados
					,TEMP.Nome_Pais
					,DATE(TEMP.Data_Inicio_Vigencia) as Data_Inicio_Vigencia 
					,DATE(TEMP.Data_Fim_Vigencia) as Data_Fim_Vigencia
           FROM   TEMP_VERSIONA TEMP )
     ; # CREATE TEMPORARY TABLE VERSIONA_TRATADA 
    
    INSERT Internet_db.Pais (
                    id_Pais
					,id_Fonte_Dados
					,Nome_Pais
                    ,Data_Inicio_Vigencia
                    ,Data_Fim_Vigencia
				)         
           SELECT  
                    id_Pais
					,id_Fonte_Dados
					,Nome_Pais
                    ,Data_Inicio_Vigencia
                    ,Data_Fim_Vigencia
                    
    FROM VERSIONA_TRATADA 
   ; # INSERT Internet_db.Pais
        
  
  
  END IF; # IF REG_VERSIONA > 0 THEN
  

END//


DELIMITER ; 