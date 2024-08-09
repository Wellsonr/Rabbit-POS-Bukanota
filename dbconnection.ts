// var mysql = require('mysql');
import mysql from 'mysql'
// eslint-disable-next-line @typescript-eslint/no-var-requires
require('dotenv').config()
const db = mysql.createPool({
  host: process.env.dbhost || 'localhost',
  user: process.env.dbuser || 'root',
  password: 'indonesiaraya',
  database: process.env.dbname || 'bukanota_pos',
  timezone: 'Z+7',
  connectionLimit: 10,
  multipleStatements: true,
  charset: "utf8mb4"
});
export default db;
