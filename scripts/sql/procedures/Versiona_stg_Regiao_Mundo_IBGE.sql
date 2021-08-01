DELIMITER //

CREATE DEFINER=`root`@`%` PROCEDURE `Stage`.`Versiona_stg_Regiao_Mundo_IBGE`()
BEGIN

/*
Criando em 10.07.2021, Jean Pierre

 */

DECLARE REG_VERSIONA int default 0; 
DECLARE maxID int default 0; 

DROP TEMPORARY TABLE IF EXISTS Stage.TEMP_VERSIONA;
DROP TEMPORARY TABLE IF EXISTS Stage.VERSIONA_TRATADA;


CREATE TEMPORARY TABLE TEMP_VERSIONA AS  (

	SELECT  
		temp.id_Fonte_Dados
		,temp.id_Regiao_Mundo
		,temp.nome_Regiao_Mundo
		,temp.Data_Extracao
		,DATE(temp.Data_Inicio_Vigencia) as Data_Inicio_Vigencia
		,DATE(temp.Data_Fim_Vigencia) as Data_Fim_Vigencia

FROM 
	Stage.stg_Regiao_Mundo_IBGE as temp

        WHERE NOT EXISTS (SELECT 1 
							FROM 
								IBGE.Regiao_Mundo_IBGE as rmi
                            WHERE 
								DATE(rmi.Data_Fim_Vigencia)  = '9999-12-31'
								AND temp.id_Fonte_Dados = rmi.id_Fonte_Dados 
								AND temp.id_Regiao_Mundo = rmi.id_Regiao_Mundo
								AND temp.nome_Regiao_Mundo = rmi.nome_Regiao_Mundo
                            ) 
		ORDER BY
			temp.id_Fonte_Dados
			,temp.id_Regiao_Mundo
) ;
# END TEMP_VERSIONA 



  
  SET REG_VERSIONA = (SELECT COUNT(*) FROM TEMP_VERSIONA) ;
  
  IF REG_VERSIONA > 0 THEN

     UPDATE IBGE.Regiao_Mundo_IBGE as rmi
     SET rmi.Data_Fim_Vigencia  = CAST(CURRENT_DATE() as DATE) 
     WHERE 
		rmi.Data_Fim_Vigencia  = CAST('9999-12-31' as DATE) 
		AND EXISTS (SELECT 1 
					FROM TEMP_VERSIONA AS TEMP
					WHERE 
						TEMP.id_Fonte_Dados = rmi.id_Fonte_Dados
						AND TEMP.id_Regiao_Mundo = rmi.id_Regiao_Mundo
					)
		; # UPDATE IBGE.Regiao_Mundo_IBGE as rmi
      
     # SET maxID = (SELECT COALESCE(MAX(id_Pais), 0)  FROM IBGE.Regiao_Mundo_IBGE)  ;
     
	CREATE TEMPORARY TABLE VERSIONA_TRATADA AS  ( 
    
            SELECT  
					TEMP.id_Fonte_Dados
					,TEMP.id_Regiao_Mundo
					,TEMP.nome_Regiao_Mundo
					,TEMP.Data_Extracao
					,DATE(TEMP.Data_Inicio_Vigencia) as Data_Inicio_Vigencia 
					,DATE(TEMP.Data_Fim_Vigencia) as Data_Fim_Vigencia
           FROM   TEMP_VERSIONA TEMP )
     ; # CREATE TEMPORARY TABLE VERSIONA_TRATADA 
    
    INSERT IBGE.Regiao_Mundo_IBGE (
                    id_Fonte_Dados
					,id_Regiao_Mundo
					,nome_Regiao_Mundo
					,Data_Extracao
                    ,Data_Inicio_Vigencia
                    ,Data_Fim_Vigencia
				)         
           SELECT  
                    id_Fonte_Dados
					,id_Regiao_Mundo
					,nome_Regiao_Mundo
					,Data_Extracao
                    ,Data_Inicio_Vigencia
                    ,Data_Fim_Vigencia
                    
    FROM VERSIONA_TRATADA 
   ; # INSERT IBGE.Regiao_Mundo_IBGE
        
  
  
  END IF; # IF REG_VERSIONA > 0 THEN

  
DROP TEMPORARY TABLE IF EXISTS Stage.TEMP_VERSIONA;
DROP TEMPORARY TABLE IF EXISTS Stage.VERSIONA_TRATADA;


END//


DELIMITER ; 