DELIMITER //

CREATE DEFINER=`root`@`%` PROCEDURE `Stage`.`Versiona_stg_Population_Country`()
BEGIN

/*
Criando em 05.07.2021, Jean Pierre

 */

DECLARE REG_VERSIONA int default 0; 
DECLARE maxID int default 0; 

DROP TEMPORARY TABLE IF EXISTS Stage.TEMP_VERSIONA;
DROP TEMPORARY TABLE IF EXISTS Stage.VERSIONA_TRATADA;


CREATE TEMPORARY TABLE TEMP_VERSIONA AS  (

	SELECT  
		stg.id_Fonte_Dados
		,p.id_Pais as id_Country
		,stg.Rank_Population
		,stg.Population
		,stg.Net_Change
		,stg.Land_Area
		,stg.Ano_Referencia
		,stg.Data_Extracao
		,CAST(CURRENT_DATE() as DATE) as Data_Inicio_Vigencia
		,CAST('9999-12-31' as DATE) as Data_Fim_Vigencia

FROM 
	Stage.stg_Population_Country as stg
		inner join
	Internet_db.Pais as p on (stg.id_Fonte_Dados = p.id_Fonte_Dados AND stg.Country = p.Nome_Pais)

        WHERE p.Data_Fim_Vigencia = '9999-12-31'
		
			AND NOT EXISTS (SELECT 1 
							FROM 
								Internet_db.Population_Country as pc
                            WHERE 
								stg.id_Fonte_Dados = pc.id_Fonte_Dados 
								AND p.id_Pais = pc.id_Country  
								AND stg.Rank_Population = pc.Rank_Population 
								AND stg.Net_Change = pc.Net_Change 
								AND stg.Land_Area = pc.Land_Area 
								AND stg.Ano_Referencia = pc.Ano_Referencia 
								AND DATE(pc.Data_Fim_Vigencia)  = '9999-12-31'
                            ) 
		ORDER BY
			stg.id_Fonte_Dados ASC
			,p.id_Pais ASC
) ;
# END TEMP_VERSIONA 



  
  SET REG_VERSIONA = (SELECT COUNT(*) FROM TEMP_VERSIONA) ;
  
  IF REG_VERSIONA > 0 THEN

     UPDATE Internet_db.Population_Country as pc
     SET pc.Data_Fim_Vigencia = CAST(CURRENT_DATE() as DATE) 
     WHERE 
		pc.Data_Fim_Vigencia = CAST('9999-12-31' as DATE) 
		AND EXISTS (SELECT 1
					FROM 
						TEMP_VERSIONA AS TEMP
					WHERE 
						TEMP.id_Fonte_Dados = pc.id_Fonte_Dados
						AND TEMP.id_Country = pc.id_Country
						AND TEMP.Ano_Referencia = pc.Ano_Referencia
					)
		; # UPDATE Internet_db.Population_Country as pc
      
     
	CREATE TEMPORARY TABLE VERSIONA_TRATADA AS  ( 
    
		SELECT  
			TEMP.id_Fonte_Dados
			,TEMP.id_Country
			,TEMP.Rank_Population
			,TEMP.Population
			,TEMP.Net_Change
			,TEMP.Land_Area
			,TEMP.Ano_Referencia
			,TEMP.Data_Extracao
			,DATE(TEMP.Data_Inicio_Vigencia) as Data_Inicio_Vigencia 
			,DATE(TEMP.Data_Fim_Vigencia) as Data_Fim_Vigencia
		FROM   TEMP_VERSIONA TEMP 
	); # CREATE TEMPORARY TABLE VERSIONA_TRATADA 


    
    INSERT Internet_db.Population_Country (
		id_Fonte_Dados
		,id_Country
		,Rank_Population
		,Population
		,Net_Change
		,Land_Area
		,Ano_Referencia
		,Data_Extracao
		,Data_Inicio_Vigencia
		,Data_Fim_Vigencia
	)         
	SELECT  
		id_Fonte_Dados
		,id_Country
		,Rank_Population
		,Population
		,Net_Change
		,Land_Area
		,Ano_Referencia
		,Data_Extracao
		,Data_Inicio_Vigencia
		,Data_Fim_Vigencia
                    
    FROM VERSIONA_TRATADA 
	; # INSERT Internet_db.Population_Country
        
  
  
  END IF; # IF REG_VERSIONA > 0 THEN
  

DROP TEMPORARY TABLE IF EXISTS Stage.TEMP_VERSIONA;
DROP TEMPORARY TABLE IF EXISTS Stage.VERSIONA_TRATADA;
  
END

DELIMITER ;