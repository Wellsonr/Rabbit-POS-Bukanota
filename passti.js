const PouchDB = require('pouchdb')
const db = require('./dbconnection')
const moment = require('moment-timezone')
const fs = require('fs')
const os = require('os')
const client = require('ftp')
const prosesSettlement = (nosettlement, idOutlet, tanggal, MIDReader, TIDReader) => {
  return new Promise((resolve, reject) => {
    db.getConnection((err, conn) => {
      if (err) return reject(err);
      const promise = new Promise((resolve, reject) => {
        conn.query("SELECT o.`database`, o.userdb, o.pwddb FROM tbloutlet o WHERE o.id=? LIMIT 1", [idOutlet], (err, results) => {
          if (err) return (reject(err));
          if (results && results.length > 0) {
            return (resolve(results[0]))
          } else {
            return (reject(new Error("Outlet tidak ditemukan")))
          }
        })
      })
      promise.then(outlet => {
        const database = outlet.database
        const userdb = outlet.userdb
        const pwddb = outlet.pwddb
        const pdb = new PouchDB(`${process.env.couchdbtarget}/${database}`, {
          auth: {
            username: userdb,
            password: pwddb
          }
        })
        const tanggalDari = moment(tanggal).set({
          hour: 0,
          minute: 0,
          second: 0
        })
        const tanggalSampai = moment(tanggal).set({
          hour: 23,
          minute: 59,
          second: 59
        })
        const listPromise = []
        listPromise.push(
          pdb.find({
            selector: {
              $and: [
                { tipe: "transaksi" },
                {
                  lastJamBayar: {
                    $gte: tanggalDari.toISOString()
                  }
                },
                {
                  lastJamBayar: {
                    $lte: tanggalSampai.toISOString()
                  }
                },
                {
                  statusid: {
                    $in: [1, 20]
                  }
                }
              ]
            },
            limit: 9999
          })
        )
        listPromise.push(
          new Promise((resolve, reject) => {
            conn.query("SELECT noinvoice FROM tblsettlementpasstid WHERE date_format(tanggal,'%Y-%m-%d') = ? AND idOutlet = ?", [tanggalDari.format("YYYY-MM-DD"), idOutlet], (err, results) => {
              if (err) return (reject(err));
              return (resolve(results))
            })
          })
        )
        return Promise.all(listPromise)
      }).then(([trans, settled]) => {
        const listPassti = [];
        for (var i = 0, j = trans.docs.length; i < j; i++) {
          if (trans.docs[i].payment) {
            for (var k = 0, l = trans.docs[i].payment.length; k < l; k++) {
              const tanggal = moment(trans.docs[i].payment[k].jamin);
              const noinvoice = trans.docs[i].payment[k].noinvoice;
              const cariInvoice = settled.find(el => el.noinvoice.toUpperCase() === noinvoice.toUpperCase())
              if (!cariInvoice) {
                for (var x = 0, z = trans.docs[i].payment[k].payment.length; x < z; x++) {
                  if (trans.docs[i].payment[k].payment[x].isPassti) {
                    if (trans.docs[i].payment[k].payment[x].MIDReader.toUpperCase() === MIDReader.toUpperCase() && trans.docs[i].payment[k].payment[x].TIDReader.toUpperCase() === TIDReader.toUpperCase()) {
                      listPassti.push({
                        noinvoice,
                        tanggal,
                        kodepayment: trans.docs[i].payment[k].payment[x].kodepayment,
                        namapayment: trans.docs[i].payment[k].payment[x].namapayment,
                        amount: trans.docs[i].payment[k].payment[x].amount,
                        nomorKartu: trans.docs[i].payment[k].payment[x].nomorKartu,
                        transRef: trans.docs[i].payment[k].payment[x].transRef,
                        transaksiLog: trans.docs[i].payment[k].payment[x].transaksiLog,
                        MIDReader: trans.docs[i].payment[k].payment[x].MIDReader,
                        TIDReader: trans.docs[i].payment[k].payment[x].TIDReader,
                        balance: trans.docs[i].payment[k].payment[x].balance
                      })
                    }
                  }
                }
              }
            }
          }
        }
        return listPassti;
      }).then(listData => {
        return new Promise((resolve, reject) => {
          conn.query("SELECT * FROM tblsettlementpassti WHERE nosettlement = ? AND statusid = 1", [nosettlement], (err, results) => {
            if (err) return reject(err);
            if (results && results.length > 0) {
              return (resolve({
                dataStatement: results[0],
                listData
              }))
            } else {
              return (reject(new Error("Transaksi Settlement tidak ditemukan atau telah diproses")))
            }
          })
        })
      }).then((data) => {
        const tanggalSettlement = moment(data.dataStatement.jamajukan).tz("Asia/Jakarta")
        const jumlahTrans = data.listData.length
        const loop = Math.ceil(jumlahTrans / 999)
        let sisaTrans = jumlahTrans;
        const listSettlement = [];
        for (var i = 0; i < loop; i++) {
          const namaFileSettlement = `${tanggalSettlement.format("YYYYMMDDHHmmss")}${MIDReader.pad(16)}${TIDReader.pad(8)}01${(i + 1).pad(3)}.txt`
          let hitung = sisaTrans > 999 ? 999 : sisaTrans;
          let listTrans = data.listData.slice(i * 999, ((i * 999) + hitung))
          const jumlah = Math.round(listTrans.map(el => el.amount).reduce((a, b) => a + b, 0))
          const stream = fs.createWriteStream(`${os.tmpdir()}\\${namaFileSettlement}`);
          stream.once('open', fd => {
            stream.write(`${hitung.pad(3)}${jumlah.pad(10)}\n`)
            for (var j = 0, k = listTrans.length; j < k; j++) {
              stream.write(`${listTrans[j].transaksiLog}\n`)
            }
            stream.end();
          })
          sisaTrans -= hitung;
          listSettlement.push(namaFileSettlement)
        }
        return {
          listSettlement,
          listData: data.listData
        }
      }).then(({ listSettlement, listData }) => {
        const c = new client()
        try {
          c.connect({
            host: process.env.ftphost || 'monkeypos.com',
            port: process.env.ftpport || 21,
            user: process.env.ftpuser || 'anonymous',
            password: process.env.ftppwd || 'anonymous@'
          })
        } catch (err) {
          throw err;
        }
        return {
          c,
          listSettlement,
          listData
        }

      }).then(({ c, listSettlement, listData }) => {
        const listPromise = []
        for (var i = 0, j = listSettlement.length; i < j; i++) {
          listPromise.push(new Promise((resolve, reject) => {
            c.put(`${os.tmpdir()}\\${listSettlement[i]}`, `/Request/${listSettlement[i]}`, false, (err) => {
              if (err) {
                c.end();
                return (reject(err))
              }
              resolve()
            })
          })
          )
        }
        return Promise.all(listPromise).then(() => {
          return {
            c,
            listData
          }
        });
      }).then(({ c, listData }) => {
        try {
          c.end()
        } catch (err) {
          throw err;
        }
        return listData
      }).then((listData) => {
        return new Promise((resolve, reject) => {

          const listPromise = []
          conn.beginTransaction(() => {
            listPromise.push(
              new Promise((resolve, reject) => {
                conn.query("UPDATE tblsettlementpassti SET statusid=20, userupt='SYSTEM', jamupt=NOW(), jamproses=NOW() WHERE nosettlement = ?", [nosettlement], (err, results => {
                  if (err) return (reject(err))
                  return (resolve(true))
                }))
              })
            )
            for (var i = 0, j = listData.length; i < j; i++) {
              listPromise.push(
                new Promise((resolve, reject) => {
                  conn.query("INSERT INTO tblsettlementpasstid (nosettlement,noinvoice,tanggal,kodepayment,namapayment,amount,nomorKartu,transRef,transaksiLog,MIDReader,TIDReader,balance,idOutlet) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", [nosettlement, listData[i].noinvoice, moment(listData[i].tanggal).format("YYYY-MM-DD"), listData[i].kodepayment, listData[i].namapayment, +listData[i].amount, listData[i].nomorKartu, listData[i].transRef, listData[i].transaksiLog, listData[i].MIDReader, listData[i].TIDReader, +listData[i].balance, idOutlet], (err, results) => {
                    if (err) return (reject(err))
                    if (results && results.affectedRows > 0) {
                      return (resolve(true))
                    } else {
                      return (reject(new Error("Gagal menyimpan data")))
                    }
                  })
                })
              )
            }
            Promise.all(listPromise).then(() => {
              conn.commit((err) => {
                if (err) {
                  conn.rollback(() => {
                    return (reject(err))
                  })
                }
                return (resolve());
              })
            }).catch(err => {
              conn.rollback(() => {
                return (reject(err))
              })
            })
          })
        })
      }).then(() => {
        conn.release();
        console.log(`${nosettlement} processed`)
        return (resolve(true))
      }).catch(err => {
        console.log("ERROR", err)
        conn.release();
        return (reject(err))
      })
    })
  })
}

exports.prosesSettlement = prosesSettlement