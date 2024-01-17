// var mysql = require('mysql');
import mysql from 'mysql'
const createMysql = (dbhost: string, dbuser: string, dbpwd: string, dbname: string) => {
return mysql.createConnection({
    host: dbhost,
    user: dbuser,
    password: dbpwd,
    database: dbname || 'homanpos',
    timezone: 'Z+7',
    multipleStatements: true,
    charset: "utf8mb4"
  })
}
export default createMysql;
