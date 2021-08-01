SELECT *
FROM mysql.`user` u ;

/*
UPDATE mysql.`user` 
SET Host = '%'
WHERE `User` = 'root'

FLUSH PRIVILEGES;

*/


/*
--Criar usuário no servidor SQL
CREATE USER 'etl'@'%' IDENTIFIED BY 'acessoetl1436';


--Liberar acesso remoto ao banco de dados através do servidor SQL
GRANT ALL PRIVILEGES ON *.* TO etl@'%' ;
FLUSH PRIVILEGES;
*/