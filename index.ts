require('./polyfill')
import './env';
// const amqp = require('amqplib')
import amqp from 'amqplib'
// const { outlet, idtrans, tanggal, kas, kastanggal } = require('./outlet')
import out = require('./outlet')
// const { welcomeMail, deleteMail } = require('./mailing')
// import { welcomeMail, deleteMail } from './mailing'
import { prosesSettlement } from './passti'
import ses = require('./session');
// const { prosesSettlement } = require('./passti')
const QUEUE_NAME = `bukanota_POS`
const TRANSAKSI = 'transaksi'
const EMAILOUTLET = "EMAILOUTLET"
const HAPUSOUTLET = 'HAPUSOUTLET'
const PROSESSETTLEMENT = "PROSESSETTLEMENT"
const RECALCULATE = 'recalculate'
const RECALCULATETANGGAL = 'recalculatetanggal'
const RECALCULATEKAS = 'recalculatekas'
const RECALCULATEKASTANGGAL = 'recalculatekastanggal'
const RECALCULATESESSIONTANGGAL = 'recalculatesessiontanggal'
const SYNCSESSSION = 'syncsession'
const IMPORTTRANSAKSI = 'importtransaksi'


const { outlet, idtrans, tanggal, kas, kastanggal, sessiontanggal, processImportTransaksi } = out
const { syncSession } = ses;

interface MysqlInfo {
  user: string,
  password: string,
  host: string,
  dbname: string,
}

const mqHost = process.env.AMQP_URL || 'localhost'
amqp.connect(`amqp://bukanota_admin:TXsrHHFpy95ISjvw@${mqHost}`)
  .then(conn => {
    return conn.createChannel().then(ch => {
      ch.prefetch(1);
      const ok = ch.assertQueue(QUEUE_NAME, { durable: true })
      return ok.then(() => {
        return ch.consume(QUEUE_NAME, msg => {
          const parsed = JSON.parse(msg.content.toString())
          console.log(parsed);
          switch (parsed.tipe) {
            case TRANSAKSI:
              outlet(parsed.dbname).then(() => {
                console.log("FINISH")
                ch.ack(msg)
              }).catch(err => {
                console.warn(err)
                ch.reject(msg, true)
              })
              break;
            case EMAILOUTLET:
              console.log("FUNCTION DEPRECATED")
              ch.ack(msg)
              break;
            case HAPUSOUTLET:
              console.log("FUNCTION DEPRECATED")
              ch.ack(msg);
              break;
            case PROSESSETTLEMENT:
              prosesSettlement(parsed.nosettlement, parsed.idOutlet, parsed.tanggal, parsed.MIDReader, parsed.TIDReader).then(() => {
                ch.ack(msg);
              }).catch(err => {
                console.warn(err)
                ch.reject(msg, true)
              })
              break;
            case RECALCULATE:
              idtrans(parsed.kodeoutlet, parsed.idtrans).then(() => {
                ch.ack(msg)
              }).catch(err => {
                console.warn(err)
                ch.reject(msg, true)
              })
              break;
            case RECALCULATETANGGAL:
              tanggal(parsed.kodeoutlet, parsed.tanggal).then(() => {
                ch.ack(msg)
              }).catch(err => {
                console.warn(err)
                ch.reject(msg, true)
              })
              break;
            case RECALCULATEKAS:
              kas(parsed.kodeoutlet, parsed.idtrans).then(() => {
                ch.ack(msg)
              }).catch(err => {
                console.warn(err)
                ch.reject(msg, true)
              })
              break;
            case RECALCULATEKASTANGGAL:
              kastanggal(parsed.kodeoutlet, parsed.tanggal).then(() => {
                ch.ack(msg)
              }).catch(err => {
                console.warn(err)
                ch.reject(msg, true)
              })
              break;
            case RECALCULATESESSIONTANGGAL:
              sessiontanggal(parsed.kodeoutlet, parsed.tanggal).then(() => {
                ch.ack(msg);
              }).catch(err => {
                console.warn(err);
                ch.reject(msg, true);
              });
              break;
            case SYNCSESSSION:
              syncSession(parsed.data, parsed.database, parsed.userdb, parsed.pwddb)
                .then(() => {
                  ch.ack(msg);
                })
                .catch(err => {
                  console.warn(err);
                  ch.reject(msg, true);
                });
                break;
            case IMPORTTRANSAKSI:
                processImportTransaksi(parsed.kodeoutlet, parsed.idtrans).then(() => {
                  ch.ack(msg)
                }).catch(err => {
                  console.warn(err);
                  ch.reject(msg, true);
                });
              break;
            default: ch.ack(msg);
          }
        }, { noAck: false })
      }).then(() => {
        console.log('* Waiting for messages. Ctrl+C to exit')
      })
    }).catch(err => {
      console.error(err.message)
    })
  }).catch(err => {
    console.log(err.message)
  })