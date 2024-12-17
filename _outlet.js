const db = require('./dbconnection')
const moment = require('moment-timezone');
const PouchDB = require('pouchdb');
const PouchDBFind = require('pouchdb-find');
const fetch = require('node-fetch');
const Cosinesimilarity = require('./Cosinesimilarity')
require('dotenv').config();
PouchDB.plugin(PouchDBFind);

const getListPaymentOutlet = (cdb) => {
  return new Promise((resolve, reject) => {
    cdb.find({
      selector: {
        _id: {
          $gt: null
        },
        tipe: 'payment',
        namapayment: {
          $gt: null
        }
      },
      limit: 9999
    }).then(({ docs }) => {
      return (resolve(docs));
    }).catch(err => reject(err))
  })
}

const getListKategoriRetail = (cdb) => {
  return new Promise((resolve, reject) => {
    cdb.find({
      selector: {
        _id: {
          $gt: null
        },
        tipe: "kategori",
        namakategori: {
          $gt: null
        }
      },
      limit: 9999
    }).then(({ docs }) => {
      return (resolve(docs))
    }).catch(reject)
  })
}

const getListKategori = (cdb) => {
  return new Promise((resolve, reject) => {
    cdb.find({
      selector: {
        _id: {
          $gt: null
        },
        tipe: "kategori",
        namakategori: {
          $gt: null
        }
      },
      limit: 9999
    }).then(({ docs }) => {
      return (resolve(docs))
    }).catch(reject)
  })
}

const getListTopping = (cdb) => {
  return new Promise((resolve, reject) => {
    cdb.find({
      selector: {
        _id: {
          $gt: null
        },
        tipe: "topping",
        namatopping: {
          $gt: null
        }
      },
      limit: 9999
    }).then(({ docs }) => {
      return (resolve(docs))
    }).catch(reject)
  })
}

const getListSubKategori = (cdb) => {
  return new Promise((resolve, reject) => {
    cdb.find({
      selector: {
        _id: {
          $gt: null
        },
        tipe: "subkategori",
        namasubkategori: {
          $gt: null
        }
      },
      limit: 9999
    }).then(({ docs }) => {
      return (resolve(docs))
    }).catch(reject)
  })
}

const getListBarangRetail = (cdb) => {
  return new Promise((resolve, reject) => {
    cdb.find({
      selector: {
        _id: {
          $gt: null
        },
        tipe: "barang",
        namabarang: {
          $gt: null
        }
      },
      limit: 9999
    }).then(({ docs }) => {
      return (resolve(docs))
    }).catch(reject)
  })
}

const getListBarang = (cdb) => {
  return new Promise((resolve, reject) => {
    cdb.find({
      selector: {
        _id: {
          $gt: null
        },
        tipe: "barang",
        namabarang: {
          $gt: null
        }
      },
      limit: 9999
    }).then(({ docs }) => {
      return (resolve(docs))
    }).catch(reject)
  })
}

const getLastSequence = (headofficecode, database) => {
  return new Promise((resolve, reject) => {
    db.getConnection((err, conn) => {
      if (err) return (reject(err))
      conn.query(`SELECT lastSequence FROM tblsequence WHERE headofficecode = ? AND \`database\` = ? ORDER BY lastUpdate DESC LIMIT 1`, [headofficecode, database], (err, results) => {
        conn.release();
        if (err) return (reject(err))
        if (results.length === 1) {
          return (resolve(results[0].lastSequence))
        } else {
          return (resolve(null))
        }
      })
    })
  })
}

const checkFilter = (database, cdb) => {
  return cdb.get("_design/filterc").then(resp => {
    const newFilter = function (doc) {
      return (doc.tipe === "transaksi" || doc.tipe === "session" || doc.tipe === "transkaskeluar" || doc.tipe === "transkasmasuk") && !doc._deleted;
    }.toString();
    if (resp.filters.crawlfilter !== newFilter) {
      let remoteDB = new PouchDB(`${process.env.couchdbtarget}/${database}`, {
        auth: {
          username: 'root',
          password: 'indonesiaraya'
        }
      })
      return remoteDB.put({
        _id: "_design/filterc",
        _rev: resp._rev,
        filters: {
          crawlfilter: newFilter
        }
      }).then(() => {
      }).catch(err => {
        console.log(err);
        throw err;
      }).finally(() => {
        remoteDB.close();
        remoteDB = null;
      })
    } else {
      return null
    }
  }).catch(err => {
    if (err.status === 404) {
      const newFilter = function (doc) {
        return (doc.tipe === "transaksi" || doc.tipe === "session" || doc.tipe === "kaskeluar" || doc.tipe === "kasmasuk") && !doc._deleted;
      }.toString();
      let remoteDB = new PouchDB(`${process.env.couchdbtarget}/${database}`, {
        auth: {
          username: 'root',
          password: 'indonesiaraya'
        }
      })
      return remoteDB.put({
        _id: "_design/filterc",
        filters: {
          crawlfilter: newFilter
        }
      }).catch(err => {
        console.log(err);
      }).finally(() => {
        remoteDB.close();
        remoteDB = null;
      })
    }
  });
}

const updateLastSequence = (resp, headofficecode, database) => {
  return new Promise((resolve, reject) => {
    db.getConnection((err, conn) => {
      if (err) return (reject(err));
      conn.query("INSERT INTO tblsequence (headofficecode,`database`,lastSequence) VALUE (?,?,?)", [headofficecode, database, resp.last_seq], err => {
        conn.release();
        if (err) return reject(err);
        return resolve()
      })
    })
  })
}

const hapusPreviousTrans = (conn, kodeoutlet, idtrans) => {
  return new Promise((resolve, reject) => {
    const banyakTrans = idtrans.length
    const jumlahLoop = Math.ceil(banyakTrans / 1000)
    const listPromise = []
    for (var i = 0; i < jumlahLoop; i++) {
      const split = idtrans.slice((i * 1000), (i * 1000) + 999)
      // console.log("HASIL SPLIT", i, split)
      listPromise.push(new Promise((resolve, reject) => {
        conn.query("DELETE FROM tblorderan WHERE kodeoutlet = ? AND idtrans IN (?); DELETE FROM tblorderanco WHERE kodeoutlet = ? AND idtrans IN (?); DELETE FROM tblorderancod WHERE kodeoutlet = ? AND idtrans IN (?); DELETE FROM tblorderancodpaket WHERE kodeoutlet = ? AND idtrans IN (?); DELETE FROM tblorderand WHERE kodeoutlet = ? AND idtrans IN (?); DELETE FROM tblorderandpaket WHERE kodeoutlet = ? AND idtrans IN (?); DELETE FROM tblorderanpayment WHERE kodeoutlet = ? AND idtrans IN (?); DELETE FROM tblorderanpaymentdetail WHERE kodeoutlet = ? AND idtrans IN (?); DELETE FROM tblorderanpaymentitem WHERE kodeoutlet = ? AND idtrans IN (?)", [kodeoutlet, split, kodeoutlet, split, kodeoutlet, split, kodeoutlet, split, kodeoutlet, split, kodeoutlet, split, kodeoutlet, split, kodeoutlet, split, kodeoutlet, split], (err) => {
          if (err) return (reject(err))
          return resolve()
        })
      }))
    }
    Promise.all(listPromise).then(() => {
      return resolve()
    }).catch(err => {
      return reject(err)
    })
  })
}

const processOrderan = (conn, kodeoutlet, idtrans, noinvoice, tanggal, nomeja, namacustomer, keterangan, userin, userupt, jamin, jamupt, lastJamBayar, statusid, displayHarga, cover, voidReason, userRetur, useDelivery, member) => {
  return new Promise((resolve, reject) => {
    const _lastJamBayar = lastJamBayar == null ? null : moment(lastJamBayar).format("YYYY-MM-DD HH:mm:ss")
    const memberName = member ? member.nama : undefined
    const memberAddress = member ? member.alamat : undefined
    const memberPhone = member ? member.nohandphone : undefined
    const memberZona = member ? member.zona : undefined
    conn.query("INSERT INTO tblorderan (kodeoutlet, idtrans, noinvoice, tanggal, nomeja, namacustomer, keterangan, userin, userupt, jamin, jamupt, lastJamBayar, statusid, displayHarga, cover, voidReason, userRetur, isDelivery, memberName, memberAddress, memberPhone, memberZone) VALUE (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", [kodeoutlet, idtrans, noinvoice, moment(tanggal).format("YYYY-MM-DD HH:mm:ss"), nomeja, namacustomer, keterangan, userin, userupt, moment(jamin).format("YYYY-MM-DD HH:mm:ss"), moment(jamupt).format("YYYY-MM-DD HH:mm:ss"), _lastJamBayar, statusid, displayHarga, cover, voidReason, userRetur, useDelivery, memberName, memberAddress, memberPhone, memberZona], (err) => {
      if (err) return reject(err)
      return resolve()
    })
  })
}

const processCaptainOrder = (conn, kodeoutlet, captainOrder, idtrans, noinvoice, detail, listSubkategori, listBarang, listTopping) => {
  return new Promise((resolve, reject) => {
    if (captainOrder) {
      const CO = captainOrder.map((el, index) => {
        return new Promise((resolve, reject) => {
          conn.query("INSERT INTO tblorderanco (kodeoutlet,idtrans,noinvoice, urutco, kodewaiter, userin, jamin) VALUE (?,?,?,?,?,?,?)", [kodeoutlet, idtrans, noinvoice, index + 1, el.kodewaiter, el.userin, moment(el.jamin).format("YYYY-MM-DD HH:mm:ss")], (err) => {
            if (err) return (reject(err))
            const listDetail = el.item.map(el2 => {
              return new Promise((resolve, reject) => {
                const barang = listBarang.find(el3 => el3.kodebarang.toUpperCase() === el2.kodebarang.toUpperCase() || (el3.variant || []).map(el4 => el4.kodebarang.toUpperCase()).indexOf(el2.kodebarang.toUpperCase()) > -1)
                const kodesubkategori = el2.kodesubkatgori ? el2.kodesubkategori : barang && barang.kodesubkategori ? barang.kodesubkategori : null
                const subkategori = kodesubkategori ? listSubkategori.find(el3 => el3.kodesubkategori.toUpperCase() === kodesubkategori.toUpperCase()) : null
                const namasubkategori = subkategori && subkategori.namasubkategori ? subkategori.namasubkategori : null
                const dataBarang = detail.find(el => el.kodebarang.toUpperCase() === el2.kodebarang.toUpperCase())
                const harga = dataBarang && dataBarang.harga ? dataBarang.harga : 0;
                const discountPercent = dataBarang && dataBarang.discountPercent ? dataBarang.discountPercent : 0;
                const jumlah = (el2.qty * harga) * (100 - discountPercent) / 100
                const isPaket = el2.detail && el2.detail.length > 0
                const isVariant = el2.isVariant || false
                conn.query("INSERT INTO tblorderancod (kodeoutlet,idtrans, noinvoice, urutco, kodebarang,namabarang,qty,harga,discountPercent,jumlah, kodesubkategori, namasubkategori,isPaket, `index`, isTopping, isVariant) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,0,?)", [kodeoutlet, idtrans, noinvoice, index + 1, el2.kodebarang, el2.namabarang, el2.qty, harga, discountPercent, jumlah, kodesubkategori, namasubkategori, isPaket, el2.index, isVariant], (err) => {
                  if (err) return (reject(err))
                  const listOrderTopping = el2.listOrderTopping || []
                  if (listOrderTopping.length > 0) {
                    const topping = listOrderTopping.map(top => {
                      return new Promise((resolve, reject) => {
                        const topping = listTopping.find(tp => tp.kodetopping.toUpperCase() === top.kodetopping.toUpperCase())
                        const kodesubkategoritop = topping && topping.kodesubkategori ? topping.kodesubkategori : null
                        const subkategoritop = kodesubkategoritop ? listSubkategori.find(el3 => el3.kodesubkategori.toUpperCase() === kodesubkategoritop.toUpperCase()) : null
                        const namasubkategoritop = subkategoritop && subkategoritop.namasubkategori ? subkategoritop.namasubkategori : null
                        conn.query("INSERT INTO tblorderancod (kodeoutlet,idtrans,noinvoice,urutco, kodebarang,namabarang,qty, harga,discountPercent, jumlah,kodesubkategori, namasubkategori,isPaket,`index`,isTopping,isVariant,kodebarangtopping,namabarangtopping) VALUE (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", [kodeoutlet, idtrans, noinvoice, index + 1, top.kodetopping, top.namatopping, el2.qty * top.qty, top.harga, 0, (el2.qty * top.qty) * top.harga, kodesubkategoritop, namasubkategoritop, 0, el2.index, true, false, el2.kodebarang, el2.namabarang], (err => {
                          if (err) return reject(err)
                          return resolve()
                        }))
                      })
                    })
                    Promise.all(topping).then(resolve)
                  } else return (resolve())
                })
              })
            })
            let listItemPaket = []
            for (var i = 0, j = el.item.length; i < j; i++) {
              if (el.item[i].detail) {
                listItemPaket = [
                  ...listItemPaket,
                  ...el.item[i].detail.map(el3 => {
                    return {
                      ...el3,
                      kodepaket: el.item[i].kodebarang
                    }
                  })
                ]
              } else {
                listItemPaket = [
                  ...listItemPaket,
                  el.item[i]
                ]
              }
            }
            const listPaket = listItemPaket.map(el2 => {
              return new Promise((resolve, reject) => {
                const barang = listBarang.find(el3 => el3.kodebarang.toUpperCase() === el2.kodebarang.toUpperCase() || (el3.variant || []).map(el4 => el4.kodebarang.toUpperCase()).indexOf(el2.kodebarang.toUpperCase()) > -1)
                const kodesubkategori = el2.kodesubkatgori ? el2.kodesubkategori : barang && barang.kodesubkategori ? barang.kodesubkategori : null
                const subkategori = kodesubkategori ? listSubkategori.find(el3 => el3.kodesubkategori.toUpperCase() === kodesubkategori.toUpperCase()) : null
                const namasubkategori = subkategori && subkategori.namasubkategori ? subkategori.namasubkategori : null
                const dataBarang = detail.find(el => el.kodebarang.toUpperCase() === el2.kodebarang.toUpperCase())
                const harga = dataBarang && dataBarang.harga ? dataBarang.harga : 0;
                const discountPercent = dataBarang && dataBarang.discountPercent ? dataBarang.discountPercent : 0;
                const jumlah = (el2.qty * harga) * (100 - discountPercent) / 100
                const kodepaket = el2.kodepaket ? el2.kodepaket : null
                conn.query("INSERT INTO tblorderancodpaket (kodeoutlet,idtrans, noinvoice, urutco, kodebarang,namabarang,qty,harga,discountPercent,jumlah,kodesubkategori,namasubkategori,kodepaket) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", [kodeoutlet, idtrans, noinvoice, index + 1, el2.kodebarang, el2.namabarang, el2.qty, harga, discountPercent, jumlah, kodesubkategori, namasubkategori, kodepaket], (err) => {
                  if (err) return (reject(err))
                  const listOrderTopping = el2.listOrderTopping || []
                  if (listOrderTopping.length > 0) {
                    const topping = listOrderTopping.map(top => {
                      return new Promise((resolve, reject) => {
                        const topping = listTopping.find(tp => tp.kodetopping.toUpperCase() === top.kodetopping.toUpperCase())
                        const kodesubkategoritop = topping && topping.kodesubkategori ? topping.kodesubkategori : null
                        const subkategoritop = kodesubkategoritop ? listSubkategori.find(el3 => el3.kodesubkategori.toUpperCase() === kodesubkategoritop.toUpperCase()) : null
                        const namasubkategoritop = subkategoritop && subkategoritop.namasubkategori ? subkategoritop.namasubkategori : null
                        conn.query("INSERT INTO tblorderancodpaket (kodeoutlet,idtrans,noinvoice,urutco, kodebarang,namabarang,qty, harga,discountPercent, jumlah,kodesubkategori, namasubkategori,`index`,isTopping,isVariant,kodebarangtopping,namabarangtopping, kodepaket) VALUE (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", [kodeoutlet, idtrans, noinvoice, index + 1, top.kodetopping, top.namatopping, el2.qty * top.qty, top.harga, 0, (el2.qty * top.qty) * top.harga, kodesubkategoritop, namasubkategoritop, el2.index, true, false, el2.kodebarang, el2.namabarang, kodepaket], (err => {
                          if (err) return reject(err)
                          return resolve()
                        }))
                      })
                    })
                    Promise.all(topping).then(resolve)
                  } else return (resolve())
                })
              })
            })
            Promise.all([
              ...listDetail,
              ...listPaket
            ]).then(resp => {
              return (resolve())
            }).catch(err => {
              return (reject(err))
            })
          })
        })
      })
      Promise.all(CO).then(() => {
        return (resolve())
      }).catch(err => {
        return (reject(err))
      })
    } else {
      return resolve();
    }
  })
}

const insertTrans = (conn, listDataDet) => {
  return new Promise((resolveJurnalPOS, rejectJurnalPOS) => {
    return new Promise((resolveJurnalH, rejectJurnalH) => {
      const listPromise = listDataDet.map(el => {
        return new Promise((resolveInsertH, rejectInsertH) => {
          if (el.statusid === 20) {
            conn.query('INSERT INTO tbltransh (notrans, tanggal, userin, userupt, jam, jamupt, status, periode, kettrans) VALUE (?,?,?,?,NOW(),NOW(),20,?,\'Penjualan POS\')', [el.kodeorderan, moment(el.tanggal).format('YYYY-MM-DD HH:mm:ss'), el.userin, el.userin, moment(el.tanggal).format('YYYYMM')], (err, results) => {
              if (err) return rejectInsertH(err);
              if (results && results.affectedRows > 0) return resolveInsertH();
              else return rejectInsertH(new Error('Gagal menyimpan header jurnal penjualan POS'));
            });
          } else return resolveInsertH();
        });
      });
      Promise
        .all(listPromise)
        .then(() => resolveJurnalH())
        .catch(err => rejectJurnalH(err));
    })
      .then(() => {
        return new Promise((resolveProsesDetail, rejectProsesDetail) => {
          return new Promise((resolveJurnalD, rejectJurnalD) => {
            const listPromise = listDataDet.map(el => {
              return new Promise((resolveInsertJurnalD, rejectInsertJurnalD) => {
                const jurnalKas = new Promise((resolveKas, rejectKas) => {
                  new Promise((resolveIdprKas, rejectIdprKas) => {
                    conn.query('SELECT idprdebet FROM tblpayment WHERE kodepayment = ? LIMIT 1', [el.kodepayment], (err, results) => {
                      if (err) return rejectIdprKas(err);
                      if (results && results.length > 0) {
                        const [{ idprdebet }] = results;
                        return resolveIdprKas(idprdebet);
                      } else return rejectIdprKas(new Error('Data Tidak Ditemukan, jurnalKas'));
                    });
                  })
                    .then(idprdebet => {
                      return new Promise((resolveInsert, rejectInsert) => {
                        // const hasReconcile = listJurnalReconcile.find(rc => rc.notrans.trim().toLowerCase() === el.kodeorderan.trim().toLowerCase() && rc.idpr.toString() === idprdebet && rc.decs.trim().toLowerCase() === 'pos payment' && rc.amount === el.jumlah);
                        const notransreconcile = "";
                        conn.query('INSERT INTO tbltransd (notrans, tanggal, statusid, idpr, ccy, decs, amount, kodeclient, typetrans, jenisclient, nobaris, nobukti, kodedevision, eqv, voucher, notransreconcile) VALUES (?,?,20,?,\'IDR\',\'POS Payment\',?,?,\'C\',\'\',1,?,?,\'\',\'\',?);', [el.noinvoice, moment(el.tanggal).format('YYYY-MM-DD HH:mm:ss'), idprdebet, el.total, el.idcust || "", el.noinvoice, el.kodegudang || "GEN0001", notransreconcile], (err, results) => {
                          if (err) return rejectInsert(err);
                          if (results && results.affectedRows > 0) return resolveInsert();
                          else return rejectInsert(new Error('Gagal menyimpan jurnal kas'));
                        });
                      });
                    })
                    .then(() => resolveKas())
                    .catch(err => rejectKas(err));
                });
                const jurnalPendapatan = new Promise((resolvePendapatan, rejectPendapatan) => {
                  new Promise < string > ((resolveIdprPendapatan, rejectIdprPendapatan) => {
                    conn.query('SELECT idprkredit FROM tblpayment WHERE kodepayment = ? LIMIT 1', [el.kodepayment], (err, results) => {
                      if (err) return rejectIdprPendapatan(err);
                      if (results && results.length > 0) {
                        const [{ idprkredit }] = results;
                        return resolveIdprPendapatan(idprkredit);
                      } else return rejectIdprPendapatan(new Error('Data Tidak Ditemukan, jurnalPendapatan'));
                    });
                  })
                    .then(idprkredit => {
                      return new Promise((resolveInsert, rejectInsert) => {
                        // const hasReconcile = listJurnalReconcile.find(rc => rc.notrans.trim().toLowerCase() === el.kodeorderan.trim().toLowerCase() && rc.idpr.toString() === idprkredit && rc.decs.trim().toLowerCase() === 'sales pos' && rc.amount === (el.jumlah - el.serv - el.tax) * -1);
                        const notransreconcile = "";
                        conn.query('INSERT INTO tbltransd (notrans, tanggal, statusid, idpr, ccy, decs, amount, kodeclient, typetrans, jenisclient, nobaris, nobukti, kodedevision, eqv, voucher, notransreconcile) VALUES (?,?,20,?,\'IDR\',\'Sales POS\',?,\'\',\'GJ\',\'\',2,?,?,\'\',\'\',?);', [el.noinvoice, moment(el.tanggal).format('YYYY-MM-DD HH:mm:ss'), idprkredit, (el.total - el.serviceAmount - el.taxAmount) * -1, el.noinvoice, el.kodegudang || "GEN0001", notransreconcile], (err, results) => {
                          if (err) return rejectInsert(err);
                          if (results && results.affectedRows > 0) return resolveInsert();
                          else return rejectInsert(new Error('Gagal menyimpan jurnal kas'));
                        });
                      });
                    })
                    .then(() => resolvePendapatan())
                    .catch(err => rejectPendapatan(err));
                });
                const jurnalService = new Promise((resolveService, rejectService) => {
                  new Promise((resolveIdprService, rejectIdprService) => {
                    conn.query('SELECT nilai FROM tblcomp2 WHERE nama = \'prservicespos\' LIMIT 1', (err, results) => {
                      if (err) return rejectIdprService(err);
                      if (results && results.length > 0) {
                        const [{ nilai }] = results;
                        return resolveIdprService(nilai);
                      } else return rejectIdprService(new Error('Data Tidak Ditemukan, jurnalService'));
                    });
                  })
                    .then(idprservice => {
                      return new Promise((resolveInsert, rejectInsert) => {
                        // const hasReconcile = listJurnalReconcile.find(rc => rc.notrans.trim().toLowerCase() === el.kodeorderan.trim().toLowerCase() && rc.idpr.toString() === idprservice && rc.decs.trim().toLowerCase() === 'service charge' && rc.amount === el.serv * -1);
                        const notransreconcile = "";
                        conn.query('INSERT INTO tbltransd (notrans, tanggal, statusid, idpr, ccy, decs, amount, kodeclient, typetrans, jenisclient, nobaris, nobukti, kodedevision, eqv, voucher, notransreconcile) VALUES (?,?,20,?,\'IDR\',\'Service Charge\',?,\'\',\'GJ\',\'\',3,?,?,\'\',\'\',?);', [el.noinvoice, moment(el.tanggal).format('YYYY-MM-DD HH:mm:ss'), idprservice, el.serviceAmount * -1, el.noinvoice, el.kodegudang || "GEN0001", notransreconcile], (err, results) => {
                          if (err) return rejectInsert(err);
                          if (results && results.affectedRows > 0) return resolveInsert();
                          else return rejectInsert(new Error('Gagal menyimpan jurnal kas'));
                        });
                      });
                    })
                    .then(() => resolveService())
                    .catch(err => rejectService(err));
                });
                const jurnalTax = new Promise((resolveTax, rejectTax) => {
                  new Promise((resolveIdprTax, rejectIdprTax) => {
                    conn.query('SELECT nilai FROM tblcomp2 WHERE nama = \'prtaxpos\' LIMIT 1', (err, results) => {
                      if (err) return rejectIdprTax(err);
                      if (results && results.length > 0) {
                        const [{ nilai }] = results;
                        return resolveIdprTax(nilai);
                      } else return rejectIdprTax(new Error('Data Tidak Ditemukan, jurnalTax'));
                    });
                  })
                    .then(idprtax => {
                      return new Promise((resolveInsert, rejectInsert) => {
                        // const hasReconcile = listJurnalReconcile.find(rc => rc.notrans.trim().toLowerCase() === el.kodeorderan.trim().toLowerCase() && rc.idpr.toString() === idprtax && rc.decs.trim().toLowerCase() === 'tax charge' && rc.amount === el.tax * -1);
                        const notransreconcile = "";
                        conn.query('INSERT INTO tbltransd (notrans, tanggal, statusid, idpr, ccy, decs, amount, kodeclient, typetrans, jenisclient, nobaris, nobukti, kodedevision, eqv, voucher, notransreconcile) VALUES (?,?,20,?,\'IDR\',\'Tax Charge\',?,\'\',\'GJ\',\'\',4,?,?,\'\',\'\',?);', [el.noinvoice, moment(el.tanggal).format('YYYY-MM-DD HH:mm:ss'), idprtax, el.taxAmount * -1, el.noinvoice, el.kodegudang || "GEN0001", notransreconcile], (err, results) => {
                          if (err) return rejectInsert(err);
                          if (results && results.affectedRows > 0) return resolveInsert();
                          else return rejectInsert(new Error('Gagal menyimpan jurnal kas'));
                        });
                      });
                    })
                    .then(() => resolveTax())
                    .catch(err => rejectTax(err));
                });
                const jurnalPersediaan = new Promise((resolvePersediaan, rejectPersediaan) => {
                  new Promise((resolveIdprPersediaan, rejectIdprPersediaan) => {
                    conn.query('SELECT nilai FROM tblcomp2 WHERE nama = \'idpersediaan\' LIMIT 1', (err, results) => {
                      if (err) return rejectIdprPersediaan(err);
                      if (results && results.length > 0) {
                        const [{ nilai }] = results;
                        return resolveIdprPersediaan(nilai);
                      } else return rejectIdprPersediaan(new Error('Data Tidak Ditemukan, jurnalPersediaan'));
                    });
                  })
                    .then(idpersediaan => {
                      return new Promise((resolveInsert, rejectInsert) => {
                        // const hasReconcile = listJurnalReconcile.find(rc => rc.notrans.trim().toLowerCase() === el.kodeorderan.trim().toLowerCase() && rc.idpr.toString() === idpersediaan && rc.decs.trim().toLowerCase() === 'pos inventory' && rc.amount === el.totalpr * -1);
                        const notransreconcile = "";
                        conn.query('INSERT INTO tbltransd (notrans, tanggal, statusid, idpr, ccy, decs, amount, kodeclient, typetrans, jenisclient, nobaris, nobukti, kodedevision, eqv, voucher, notransreconcile) VALUES (?,?,20,?,\'IDR\',\'POS Inventory\',?,\'\',\'GJ\',\'\',5,?,?,\'\',\'\',?);', [el.noinvoice, moment(el.tanggal).format('YYYY-MM-DD HH:mm:ss'), idpersediaan, 0, el.noinvoice, el.kodegudang || "GEN0001", notransreconcile], (err, results) => {
                          if (err) return rejectInsert(err);
                          if (results && results.affectedRows > 0) return resolveInsert();
                          else return rejectInsert(new Error('Gagal menyimpan jurnal kas'));
                        });
                      });
                    })
                    .then(() => resolvePersediaan())
                    .catch(err => rejectPersediaan(err));
                });
                const jurnalHPP = new Promise((resolveHpp, rejectHpp) => {
                  new Promise((resolveIdprHpp, rejectIdprHpp) => {
                    conn.query('SELECT nilai FROM tblcomp2 WHERE nama = \'idhpp\' LIMIT 1', (err, results) => {
                      if (err) return rejectIdprHpp(err);
                      if (results && results.length > 0) {
                        const [{ nilai }] = results;
                        return resolveIdprHpp(nilai);
                      } else return rejectIdprHpp(new Error('Data Tidak Ditemukan, jurnalHPP'));
                    });
                  })
                    .then(idhpp => {
                      return new Promise((resolveInsert, rejectInsert) => {
                        // const hasReconcile = listJurnalReconcile.find(rc => rc.notrans.trim().toLowerCase() === el.kodeorderan.trim().toLowerCase() && rc.idpr.toString() === idhpp && rc.decs.trim().toLowerCase() === 'pos cogs' && rc.amount === el.totalpr);
                        const notransreconcile = "";
                        conn.query('INSERT INTO tbltransd (notrans, tanggal, statusid, idpr, ccy, decs, amount, kodeclient, typetrans, jenisclient, nobaris, nobukti, kodedevision, eqv, voucher, notransreconcile) VALUES (?,?,20,?,\'IDR\',\'POS COGS\',?,\'\',\'GJ\',\'\',6,?,?,\'\',\'\',?);', [el.noinvoice, moment(el.tanggal).format('YYYY-MM-DD HH:mm:ss'), idhpp, 0, el.noinvoice, el.kodegudang || "GEN0001", notransreconcile], (err, results) => {
                          if (err) return rejectInsert(err);
                          if (results && results.affectedRows > 0) return resolveInsert();
                          else return rejectInsert(new Error('Gagal menyimpan jurnal kas'));
                        });
                      });
                    })
                    .then(() => resolveHpp())
                    .catch(err => rejectHpp(err));
                });
                Promise
                  .all([
                    jurnalKas,
                    jurnalPendapatan,
                    jurnalService,
                    jurnalTax,
                    jurnalPersediaan,
                    jurnalHPP,
                  ])
                  .then(() => resolveInsertJurnalD())
                  .catch(err => rejectInsertJurnalD(err));
              });
            });
            Promise.all(listPromise)
              .then(() => resolveJurnalD())
              .catch(err => rejectJurnalD(err));
          })
            .then(() => resolveProsesDetail())
            .catch(err => rejectProsesDetail(err));
        });
      })
      .then(() => resolveJurnalPOS())
      .catch(err => rejectJurnalPOS(err));
  });
};

const processItem = (conn, kodeoutlet, detail, idtrans, noinvoice, listKategori, listSubkategori, listBarang, listTopping) => {
  return new Promise((resolve, reject) => {
    if (detail) {
      const item = detail.map(el => {
        return new Promise((resolve, reject) => {
          const barang = listBarang.find(el2 => el2.kodebarang.toUpperCase() === el.kodebarang.toUpperCase() || (el2.variant || []).map(el3 => el3.kodebarang.toUpperCase()).indexOf(el.kodebarang.toUpperCase()) > -1)
          const kodekategori = barang && barang.kodekategori ? barang.kodekategori : null
          const kategori = kodekategori ? listKategori.find(el2 => el2.kodekategori.toUpperCase() === kodekategori.toUpperCase()) : null
          const namakategori = kategori && kategori.namakategori ? kategori.namakategori : null
          const kodesubkategori = el.kodesubkatgori ? el.kodesubkategori : barang && barang.kodesubkategori ? barang.kodesubkategori : null
          const subkategori = kodesubkategori ? listSubkategori.find(el2 => el2.kodesubkategori.toUpperCase() === kodesubkategori.toUpperCase()) : null
          const namasubkategori = subkategori && subkategori.namasubkategori ? subkategori.namasubkategori : null
          const isPaket = el.detail && el.detail.length > 0 || false
          const isPromo = el.isPromo != null ? el.isPromo : false
          const isVariant = el.isVariant || false
          conn.query("INSERT INTO tblorderand (kodeoutlet, idtrans, noinvoice, kodekategori, namakategori, kodesubkategori, namasubkategori, kodebarang, namabarang, qty, discountPercent, discountAmount, harga, jumlah, isPaket, isPromo, `index`, isVariant, isTopping) VALUE (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,FALSE)", [kodeoutlet, idtrans, noinvoice, kodekategori, namakategori, kodesubkategori, namasubkategori, el.kodebarang, el.namabarang, el.qty, el.discountPercent, el.discountAmount, el.harga, el.jumlah, isPaket, isPromo, el.index, isVariant], (err) => {
            if (err) return reject(err)
            const listOrderTopping = el.listOrderTopping || []
            if (listOrderTopping.length > 0) {
              const topping = listOrderTopping.map(top => {
                return new Promise((resolve, reject) => {
                  const topping = listTopping.find(tp => tp.kodetopping.toUpperCase() === top.kodetopping.toUpperCase())
                  const kodekategoritop = topping && topping.kodekategori ? topping.kodekategori : null
                  const kategoritop = kodekategoritop ? listKategori.find(el2 => el2.kodekategori.toUpperCase() === kodekategoritop.toUpperCase()) : null
                  const namakategoritop = kategoritop && kategoritop.namakategori ? kategoritop.namakategori : null
                  const kodesubkategoritop = topping && topping.kodesubkategori ? topping.kodesubkategori : null
                  const subkategoritop = kodesubkategoritop ? listSubkategori.find(el3 => el3.kodesubkategori.toUpperCase() === kodesubkategoritop.toUpperCase()) : null
                  const namasubkategoritop = subkategoritop && subkategoritop.namasubkategori ? subkategoritop.namasubkategori : null
                  conn.query("INSERT INTO tblorderand (kodeoutlet,idtrans,noinvoice,kodekategori, namakategori, kodesubkategori, namasubkategori, kodebarang,namabarang,qty,discountPercent, discountAmount, harga, jumlah,isPaket,isPromo,`index`,isTopping,isVariant,kodebarangtopping,namabarangtopping) VALUE (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", [kodeoutlet, idtrans, noinvoice, kodekategoritop, namakategoritop, kodesubkategoritop, namasubkategoritop, top.kodetopping, top.namatopping, top.qty * el.qty, 0, 0, top.harga, (el.qty * top.qty) * top.harga, 0, 0, el.index, true, false, el.kodebarang, el.namabarang], (err => {
                    if (err) return reject(err)
                    return resolve()
                  }))
                })
              })
              Promise.all(topping).then(resolve)
            } else return (resolve())
          })
        })
      })
      let listItemPaket = []
      for (var i = 0, j = detail.length; i < j; i++) {
        if (detail[i].detail) {
          listItemPaket = [
            ...listItemPaket,
            ...detail[i].detail.map(el2 => {
              return {
                ...el2,
                kodepaket: detail[i].kodebarang,
                isPromo: detail[i].isPromo
              }
            })
          ]
        } else {
          listItemPaket = [
            ...listItemPaket,
            detail[i]
          ]
        }
      }
      const listPaket = listItemPaket.map(el => {
        return new Promise((resolve, reject) => {
          const barang = listBarang.find(el2 => el2.kodebarang.toUpperCase() === el.kodebarang.toUpperCase() || (el2.variant || []).map(el3 => el3.kodebarang.toUpperCase()).indexOf(el.kodebarang.toUpperCase()) > -1)
          const kodekategori = barang && barang.kodekategori ? barang.kodekategori : null
          const kategori = kodekategori ? listKategori.find(el2 => el2.kodekategori.toUpperCase() === kodekategori.toUpperCase()) : null
          const namakategori = kategori && kategori.namakategori ? kategori.namakategori : null
          const kodesubkategori = el.kodesubkatgori ? el.kodesubkategori : barang && barang.kodesubkategori ? barang.kodesubkategori : null
          const subkategori = kodesubkategori ? listSubkategori.find(el2 => el2.kodesubkategori.toUpperCase() === kodesubkategori.toUpperCase()) : null
          const namasubkategori = subkategori && subkategori.namasubkategori ? subkategori.namasubkategori : null
          const kodepaket = el.kodepaket ? el.kodepaket : null
          const isPromo = el.isPromo != null ? el.isPromo : false
          const isVariant = el.isVariant != null ? el.isVariant : false
          conn.query("INSERT INTO tblorderandpaket (kodeoutlet, idtrans, noinvoice, kodekategori, namakategori, kodesubkategori, namasubkategori, kodebarang, namabarang, qty, discountPercent, discountAmount, harga, jumlah, kodepaket, isPromo, `index`, isVariant, isTopping) VALUE (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,FALSE)", [kodeoutlet, idtrans, noinvoice, kodekategori, namakategori, kodesubkategori, namasubkategori, el.kodebarang, el.namabarang, el.qty, el.discountPercent, el.discountAmount, el.harga, el.jumlah, kodepaket, isPromo, el.index, isVariant], (err) => {
            if (err) return reject(err)
            const listOrderTopping = el.listOrderTopping || []
            if (listOrderTopping.length > 0) {
              const topping = listOrderTopping.map(top => {
                return new Promise((resolve, reject) => {
                  const topping = listTopping.find(tp => tp.kodetopping.toUpperCase() === top.kodetopping.toUpperCase())
                  const kodekategoritop = topping && topping.kodekategori ? topping.kodekategori : null
                  const kategoritop = kodekategoritop ? listKategori.find(el2 => el2.kodekategori.toUpperCase() === kodekategoritop.toUpperCase()) : null
                  const namakategoritop = kategoritop && kategoritop.namakategori ? kategoritop.namakategori : null
                  const kodesubkategoritop = topping && topping.kodesubkategori ? topping.kodesubkategori : null
                  const subkategoritop = kodesubkategoritop ? listSubkategori.find(el3 => el3.kodesubkategori.toUpperCase() === kodesubkategoritop.toUpperCase()) : null
                  const namasubkategoritop = subkategoritop && subkategoritop.namasubkategori ? subkategoritop.namasubkategori : null
                  conn.query("INSERT INTO tblorderandpaket (kodeoutlet, idtrans, noinvoice, kodekategori, namakategori, kodesubkategori, namasubkategori, kodebarang, namabarang, qty, discountPercent, discountAmount, harga, jumlah, kodepaket, isPromo, `index`, isVariant, isTopping,kodebarangtopping,namabarangtopping) VALUE (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", [kodeoutlet, idtrans, noinvoice, kodekategoritop, namakategoritop, kodesubkategoritop, namasubkategoritop, top.kodetopping, top.namatopping, top.qty * el.qty, 0, 0, top.harga, (el.qty * top.qty) * top.harga, kodepaket, 0, el.index, false, true, el.kodebarang, el.namabarang], (err => {
                    if (err) return reject(err)
                    return resolve()
                  }))
                })
              })
              Promise.all(topping).then(resolve)
            } else return (resolve())
          })
        })
      })
      Promise.all([
        ...item,
        ...listPaket
      ]).then(() => {
        resolve()
      }).catch(reject)
    } else {
      return resolve()
    }
  })
}

const processPayment = (conn, kodeoutlet, payment, idtrans, noinvoice, listPayment, listBarang, listSubkategori, listTopping) => {
  return new Promise((resolve, reject) => {
    if (payment) {
      const hasil = payment.map((el, index) => {
        return new Promise((resolve, reject) => {
          const promoAmount = el.promoAmount != null ? el.promoAmount : 0
          conn.query("INSERT INTO tblorderanpayment (kodeoutlet,idtrans,noinvoice, urutpayment, subtotal, discountAmount, discountPercent, serviceAmount, servicePercent, taxAmount, taxPercent, total, pembulatan, kembalian, totalPayment, userin, jamin, sessionId,promoAmount, kodePromo,namaPromo,ongkir) VALUE (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", [kodeoutlet, idtrans, noinvoice, index + 1, el.subtotal, el.discountAmount, el.discountPercent, el.serviceAmount, el.servicePercent, el.taxAmount, el.taxPercent, el.total, el.pembulatan, el.kembalian, el.totalPayment, el.userin, moment(el.jamin).format("YYYY-MM-DD HH:mm:ss"), el.sessionId, promoAmount, el.kodePromo, el.namaPromo, el.ongkir], err => {
            if (err) return reject(err)
            const paymentItem = el.item.map(el2 => {
              return new Promise((resolve, reject) => {
                const barang = listBarang.find(el3 => el3.kodebarang.toUpperCase() === el2.kodebarang.toUpperCase() || (el3.variant || []).map(el4 => el4.kodebarang.toUpperCase()).indexOf(el2.kodebarang.toUpperCase()) > -1)
                const kodesubkategori = el2.kodesubkatgori ? el2.kodesubkategori : barang && barang.kodesubkategori ? barang.kodesubkategori : null
                const subkategori = kodesubkategori ? listSubkategori.find(el3 => el3.kodesubkategori.toUpperCase() === kodesubkategori.toUpperCase()) : null
                const namasubkategori = subkategori && subkategori.namasubkategori ? subkategori.namasubkategori : null
                const isPromo = el2.isPromo != null ? el2.isPromo : false
                const isVariant = el2.isVariant != null ? el2.isVariant : false
                conn.query("INSERT INTO tblorderanpaymentitem (kodeoutlet,idtrans,noinvoice,urutpayment,kodesubkategori, namasubkategori, kodebarang,namabarang,qty,discountPercent,discountAmount,harga,jumlah,isPromo,`index`, isTopping,isVariant) VALUE (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,FALSE,?)", [kodeoutlet, idtrans, noinvoice, index + 1, kodesubkategori, namasubkategori, el2.kodebarang, el2.namabarang, el2.qty, el2.discountPercent, el2.discountAmount, el2.harga, el2.jumlah, isPromo, el2.index, isVariant], err => {
                  if (err) return reject(err)
                  const listOrderTopping = el2.listOrderTopping || []
                  if (listOrderTopping.length > 0) {
                    const topping = listOrderTopping.map(top => {
                      return new Promise((resolve, reject) => {
                        const topping = listTopping.find(tp => tp.kodetopping.toUpperCase() === top.kodetopping.toUpperCase())
                        const kodesubkategoritop = topping && topping.kodesubkategori ? topping.kodesubkategori : null
                        const subkategoritop = kodesubkategoritop ? listSubkategori.find(el3 => el3.kodesubkategori.toUpperCase() === kodesubkategoritop.toUpperCase()) : null
                        const namasubkategoritop = subkategoritop && subkategoritop.namasubkategori ? subkategoritop.namasubkategori : null
                        conn.query("INSERT INTO tblorderanpaymentitem (kodeoutlet,idtrans,noinvoice,urutpayment,kodesubkategori,namasubkategori,kodebarang,namabarang,qty,discountPercent,discountAmount,harga,jumlah,isPromo,`index`,isTopping,isVariant,kodebarangtopping,namabarangtopping) VALUE (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", [kodeoutlet, idtrans, noinvoice, index + 1, kodesubkategoritop, namasubkategoritop, top.kodetopping, top.namatopping, top.qty * el2.qty, 0, 0, top.harga, (el2.qty * top.qty) * top.harga, 0, el2.index, true, false, el2.kodebarang, el2.namabarang], (err) => {
                          if (err) return reject(err)
                          return resolve()
                        })
                      })
                    })
                    Promise.all(topping).then(resolve)
                  } else return (resolve())
                })
              })
            })
            const paymentDetail = el.payment.map((el2, index2) => {
              return new Promise((resolve, reject) => {
                const payment = listPayment.find(el3 => el3.kodepayment.toUpperCase() === el2.kodepayment.toUpperCase())
                const isCard = payment && payment.isCard ? payment.isCard : false
                const isCashlez = payment && payment.isCashlez ? payment.isCashlez : false
                const isQREN = payment && payment.isQREN ? payment.isQREN : false
                conn.query("INSERT INTO tblorderanpaymentdetail (kodeoutlet,idtrans,noinvoice,urutpayment,kodepayment,namapayment,remark,amount,nomorKartu, transRef,isCard,isCashlez,isQREN) VALUE (?,?,?,?,?,?,?,?,?,?,?,?,?)", [kodeoutlet, idtrans, noinvoice, index + 1, el2.kodepayment, el2.namapayment, el2.remark, el2.amount, el2.nomorKartu, el2.transRef, isCard, isCashlez, isQREN], err => {
                  if (err) return reject(err)
                  return resolve()
                })
              })
            })
            Promise.all([
              ...paymentItem,
              ...paymentDetail
            ]).then(() => {
              return resolve()
            }).catch(reject)
          })
        })
      })
      Promise.all(hasil).then(() => {
        return (resolve())
      }).catch(reject)
    } else {
      return resolve()
    }
  })
}

const convertPhone = (val) => {
  let handphone = val
  if (val.match(/^\+62/)) handphone = val.replace(/^\+62/, '0')
  if (val.match(/^\+65/)) handphone = val.replace(/^\+62/, '')
  return handphone.trim()
}

var groupBy = function (xs, key) {
  return xs.reduce(function (rv, x) {
    (rv[x[key]] = rv[x[key]] || []).push(x);
    return rv;
  }, {});
};

const processMember = (members, cdb) => {
  return new Promise(async (resolve, reject) => {
    const sorted = members.filter(el => el).map(el => {
      return {
        ...el,
        nohandphone: convertPhone(el.nohandphone)
      }
    })
    const groupResult = groupBy(sorted, "nohandphone")
    let hitung = 0;
    for (var hp of Object.keys(groupResult)) {
      const nohandphone = convertPhone(hp);
      if (nohandphone != '') {
        await cdb.get(`member-${nohandphone}`).then(async doc => {
          const previousAlamat = doc.dataMember.map(el => el.alamat)
          let newAlamat = [];
          for (var i = 0, j = groupResult[hp].length; i < j; i++) {
            const checkAlamat = [...previousAlamat, ...newAlamat.map(el => el.alamat)]
            let max = 0.0;
            for (var k = 0, l = checkAlamat.length; k < l; k++) {
              const score = Cosinesimilarity(checkAlamat[k], groupResult[hp][i].alamat)
              if (score > max) max = score;
            }
            if (max < 0.8) {
              newAlamat = [...newAlamat, groupResult[hp][i]]
              hitung++
            }
          }
          await cdb.put({
            ...doc,
            dataMember: [
              ...doc.dataMember,
              ...newAlamat
            ]
          }).catch(reject)
        }).catch(async err => {
          if (err.status === 404) {
            let newAlamat = [];
            for (var i = 0, j = groupResult[hp].length; i < j; i++) {
              const checkAlamat = [...newAlamat]
              let max = 0.0;
              for (var k = 0, l = checkAlamat.length; k < l; k++) {
                const score = Cosinesimilarity(checkAlamat[k].alamat, groupResult[hp][i].alamat)
                if (score > max) max = score;
              }
              if (max < 0.8) {
                newAlamat = [...newAlamat, groupResult[hp][i]]
                hitung++
              }
            }
            await cdb.put({
              _id: `member-${nohandphone}`,
              dataMember: newAlamat
            }).catch(reject)
          }
        })
      }
    }
    console.log(`${hitung} alamat terproses`)
    return resolve()
  })
}

const processTransaksi = (listTransaksi, kodeoutlet, listKategori, listSubkategori, listBarang, listPayment, listTopping, cdb) => {
  return new Promise((resolve, reject) => {
    if (listTransaksi.length > 0) {
      db.getConnection((err, conn) => {
        if (err) {
          conn.release()
          return reject(err)
        }
        conn.beginTransaction((err) => {
          if (err) {
            conn.release()
            return reject(err)
          }
          conn.config.timeout = 0
          hapusPreviousTrans(conn, kodeoutlet, listTransaksi.map(el => el._id)).then(() => {
            const banyakTrans = listTransaksi.length
            const jumlahLoop = Math.ceil(banyakTrans / 1000)
            const listPromise = []
            for (var i = 0; i < jumlahLoop; i++) {
              const split = listTransaksi.slice((i * 1000), (i * 1000) + 999)
              listPromise.push(new Promise((resolve, reject) => {
                const hasilProcess = split.map(el => {
                  return new Promise((resolve, reject) => {
                    const idtrans = el._id
                    processOrderan(conn, kodeoutlet, idtrans, el.noinvoice, el.tanggal, el.nomeja, el.namacustomer, el.keterangan, el.userin, el.userupt, el.jamin, el.jamupt, el.lastJamBayar, el.statusid, el.displayHarga, el.cover, el.voidReason, el.userRetur, el.useDelivery, el.member).then(() => {
                      return processCaptainOrder(conn, kodeoutlet, el.captainOrder, idtrans, el.noinvoice, el.detail, listSubkategori, listBarang, listTopping)
                    }).then(() => {
                      return processItem(conn, kodeoutlet, el.detail, idtrans, el.noinvoice, listKategori, listSubkategori, listBarang, listTopping)
                    }).then(() => {
                      return processPayment(conn, kodeoutlet, el.payment, idtrans, el.noinvoice, listPayment, listBarang, listSubkategori, listTopping)
                    }).then(() => {
                      return insertTrans(conn, listTransaksi, el.tanggal)
                    }).then(() => {
                      return resolve();
                    }).catch(err => {
                      console.log("Error", err)
                      const fs = require('fs')
                      fs.writeFile("D:\\error.txt", JSON.stringify(err), (err2) => {
                        if (err2) console.log(err2)
                      })
                      return reject(err)
                    })
                  })
                })
                Promise.all(hasilProcess).then(resolve).catch(reject)
              }))
            }
            return Promise.all(listPromise)
          }).then(() => {
            console.log("PROCESS MEMBER")
            return processMember(listTransaksi.map(el => el.member), cdb)
          }).then(() => {
            conn.commit(() => {
              conn.release();
              return resolve()
            })
          }).catch(err => {
            console.log(err);
            conn.rollback(() => {
              conn.release();
              return reject(err)
            })
          })
        })
      })
    } else return resolve();
  })
}

const hapusPreviousSession = (conn, kodeoutlet, listSession) => {
  return new Promise((resolve, reject) => {
    conn.query("DELETE FROM tblsession WHERE kodeoutlet = ? AND sessionId IN (?)", [kodeoutlet, listSession], (err) => {
      if (err) return reject(err)
      return resolve();
    })
  })
}

const processSession = (listSession, kodeoutlet) => {
  return new Promise((resolve, reject) => {
    if (listSession.length > 0) {
      db.getConnection((err, conn) => {
        if (err) return reject(err)
        conn.beginTransaction((err) => {
          if (err) return reject(err)
          hapusPreviousSession(conn, kodeoutlet, listSession.map(el => el.sessionId)).then(() => {
            const hasilProcess = listSession.map(el => {
              return new Promise((resolve, reject) => {
                conn.query("INSERT INTO tblsession (kodeoutlet, sessionId, saldoAwal, saldoAkhir, tanggalBuka, tanggalTutup, userBuka, userTutup) VALUES (?,?,?,?,?,?,?,?)", [kodeoutlet, el.sessionId, el.saldoAwal, el.saldoAkhir, moment(el.tanggalBuka).format("YYYY-MM-DD HH:mm:ss"), el.tanggalTutup ? moment(el.tanggalTutup).format("YYYY-MM-DD HH:mm:ss") : undefined, el.userBuka, el.userTutup], (err) => {
                  if (err) return reject(err)
                  return resolve()
                })
              })
            })
            return Promise.all(hasilProcess)
          }).then(() => {
            conn.commit(() => {
              conn.release();
            })
            return resolve()
          }).catch(err => {
            conn.rollback(() => {
              conn.release();
            })
            console.log(err)
            return reject(err)
          })
        })
      })
    } else return resolve()
  })
}

const hapusPreviousKasMasuk = (conn, kodeoutlet, listTrans) => {
  return new Promise((resolve, reject) => {
    conn.query("DELETE FROM tbltranskasmasuk WHERE kodeoutlet = ? AND idtrans IN (?)", [kodeoutlet, listTrans], (err) => {
      if (err) return reject(err)
      conn.query("DELETE FROM tbltranskasmasukd WHERE kodeoutlet = ? AND idtrans IN (?)", [kodeoutlet, listTrans], (err) => {
        if (err) return reject(err)
        return resolve()
      })
    })
  })
}

const processKasMasuk = (listKasMasuk, kodeoutlet) => {
  return new Promise((resolve, reject) => {
    if (listKasMasuk.length > 0) {
      db.getConnection((err, conn) => {
        if (err) return reject(err)
        conn.beginTransaction((err) => {
          if (err) return reject(err)
          hapusPreviousKasMasuk(conn, kodeoutlet, listKasMasuk.map(el => el._id)).then(() => {
            const hasilProcess = listKasMasuk.map(el => {
              return new Promise((resolve, reject) => {
                const jamin = el.jamin != null ? moment(el.jamin).format("YYYY-MM-DD HH:mm:ss") : undefined;
                const tanggal = el.tanggal != null ? moment(el.tanggal).format("YYYY-MM-DD HH:mm:ss") : undefined
                conn.query("INSERT INTO tbltranskasmasuk (kodeoutlet,idtrans,noinvoice,sessionId,userin,jamin,tanggal) VALUE (?,?,?,?,?,?,?)", [kodeoutlet, el._id, el.noinvoice, el.sessionId, el.userin, jamin, tanggal], (err) => {
                  if (err) return reject(err)
                  const promises = el.detail.map(dt => {
                    return new Promise((resolve, reject) => {
                      conn.query("INSERT INTO tbltranskasmasukd (kodeoutlet, idtrans, noinvoice, kodebiaya, namabiaya, amount, keterangan) VALUE (?,?,?,?,?,?,?)", [kodeoutlet, el._id, el.noinvoice, dt.kodebiaya, dt.namabiaya, dt.amount, dt.keterangan], (err) => {
                        if (err) return reject()
                        else return resolve()
                      })
                    })
                  })
                  Promise.all(promises).then(resolve).catch(reject)
                })
              })
            })
            return Promise.all(hasilProcess)
          }).then(() => {
            conn.commit(() => {
              conn.release();
            })
            return resolve()
          }).catch(err => {
            conn.rollback(() => {
              conn.release();
            })
            console.log(err)
            return reject(err)
          })
        })
      })
    } else return resolve()
  })
}

const hapusPreviousKasKeluar = (conn, kodeoutlet, listTrans) => {
  return new Promise((resolve, reject) => {
    conn.query("DELETE FROM tbltranskaskeluar WHERE kodeoutlet = ? AND idtrans IN (?)", [kodeoutlet, listTrans], (err) => {
      if (err) return reject(err)
      conn.query("DELETE FROM tbltranskaskeluard WHERE kodeoutlet = ? AND idtrans IN (?)", [kodeoutlet, listTrans], (err) => {
        if (err) return reject(err)
        return resolve()
      })
    })
  })
}

const processKasKeluar = (listKasKeluar, kodeoutlet) => {
  return new Promise((resolve, reject) => {
    if (listKasKeluar.length > 0) {
      db.getConnection((err, conn) => {
        if (err) return reject(err)
        conn.beginTransaction((err) => {
          if (err) return reject(err)
          hapusPreviousKasKeluar(conn, kodeoutlet, listKasKeluar.map(el => el._id)).then(() => {
            const hasilProcess = listKasKeluar.map(el => {
              return new Promise((resolve, reject) => {
                const jamin = el.jamin != null ? moment(el.jamin).format("YYYY-MM-DD HH:mm:ss") : undefined;
                const tanggal = el.tanggal != null ? moment(el.tanggal).format("YYYY-MM-DD HH:mm:ss") : undefined
                conn.query("INSERT INTO tbltranskaskeluar (kodeoutlet,idtrans,noinvoice,sessionId,userin,jamin,tanggal) VALUE (?,?,?,?,?,?,?)", [kodeoutlet, el._id, el.noinvoice, el.sessionId, el.userin, jamin, tanggal], (err) => {
                  if (err) return reject(err)
                  const promises = el.detail.map(dt => {
                    return new Promise((resolve, reject) => {
                      conn.query("INSERT INTO tbltranskaskeluard (kodeoutlet, idtrans, noinvoice, kodebiaya, namabiaya, amount, keterangan) VALUE (?,?,?,?,?,?,?)", [kodeoutlet, el._id, el.noinvoice, dt.kodebiaya, dt.namabiaya, dt.amount, dt.keterangan], (err) => {
                        if (err) return reject()
                        else return resolve()
                      })
                    })
                  })
                  Promise.all(promises).then(resolve).catch(reject)
                })
              })
            })
            return Promise.all(hasilProcess)
          }).then(() => {
            conn.commit(() => {
              conn.release();
            })
            return resolve()
          }).catch(err => {
            conn.rollback(() => {
              conn.release();
            })
            console.log(err)
            return reject(err)
          })
        })
      })
    } else return resolve()
  })
}

const processOutlet = (headofficecode, database, kodeoutlet, namaoutlet, userdb, pwddb, useIposStock) => {
  return new Promise(async (resolve, reject) => {
    let last_seq = ''
    try {
      last_seq = await getLastSequence(headofficecode, database);
    } catch (err) {
      throw err
    }
    const cdb = new PouchDB(`${process.env.couchdbtarget}/${database}`, {
      auth: {
        username: userdb,
        password: pwddb
      }
    });
    try {
      await checkFilter(database, cdb)
    } catch (err) {
      cdb.close()
      throw err;
    }
    const host = process.env.couchdbsource || 'db.monkeypos.com'
    fetch(`https://${userdb}:${pwddb}@${host}/${database}/_changes?include_docs=true&filter=filterc/crawlfilter&since=${last_seq || 0}`).then(async resp => {
      const body = await resp.text()
      try {
        return JSON.parse(body);
      } catch (err) {
        console.warn("Error:", err);
        console.warn("Body:", body);
        return (reject(err))
      }
    }).then(async resp => {
      if (resp && resp.results) {
        console.log(`Processing ${resp.results.length} of data for ${database}`);
        if (resp.results.length > 0) {

          let listDataPayment, listKategori, listSubkategori, listBarang, listTopping;
          try {
            listDataPayment = await getListPaymentOutlet(cdb)
            listKategori = await getListKategori(cdb)
            listSubkategori = await getListSubKategori(cdb)
            listBarang = await getListBarang(cdb)
            listTopping = await getListTopping(cdb)
          } catch (err) {
            cdb.close()
            return (reject(err))
          }
          const listTransaksi = resp.results.map(el => {
            return {
              ...el.doc
            }
          }).filter(el => el.tipe === "transaksi")
          try {
            await processTransaksi(listTransaksi, kodeoutlet, listKategori, listSubkategori, listBarang, listDataPayment, listTopping, cdb)
          } catch (err) {
            console.log(err)
            cdb.close()
            return reject(err)
          }
          console.log(`${listTransaksi.length} transaksi terproses`);
          const listSession = resp.results.map(el => {
            return {
              ...el.doc
            }
          }).filter(el => el.tipe === "session")
          try {
            await processSession(listSession, kodeoutlet);
          } catch (err) {
            console.log(err)
            cdb.close()
            return (reject(err))
          }
          console.log(`${listSession.length} sesi terproses`);
          const listKasMasuk = resp.results.map(el => {
            return {
              ...el.doc
            }
          }).filter(el => el.tipe === "transkasmasuk")
          try {
            await processKasMasuk(listKasMasuk, kodeoutlet);
          } catch (err) {
            console.log(err)
            cdb.close()
            return reject(err)
          }
          console.log(`${listKasMasuk.length} kas masuk terproses`);
          const listKasKeluar = resp.results.map(el => {
            return {
              ...el.doc
            }
          }).filter(el => el.tipe === "transkaskeluar")
          try {
            await processKasKeluar(listKasKeluar, kodeoutlet);
          } catch (err) {
            console.log(err)
            cdb.close()
            return reject(err)
          }
          console.log(`${listKasKeluar.length} kas keluar terproses`);
        } else console.log(`Tidak ada transaksi yang diproses`)
        await updateLastSequence(resp, headofficecode, database)
        cdb.close();
        return (resolve());
      } else {
        return (resolve())
      }
    }).catch(err => {
      cdb.close()
      return (reject(err))
    })
  })
}

const processPaymentRetail = (conn, kodeoutlet, payment, idtrans, listBarang, listKategori) => {
  return new Promise((resolve, reject) => {
    if (payment) {
      const hasil = new Promise((resolve, reject) => {
        conn.query("INSERT INTO tblorderanpayment (kodeoutlet,idtrans,noinvoice,urutpayment,subtotal,discountAmount,discountPercent,serviceAmount,servicePercent,taxAmount,taxPercent,total,pembulatan,kembalian,totalPayment,userin,jamin,sessionId) VALUE(?,?,?,1,?,?,?,?,?,?,?,?,0,?,?,?,?,?)", [kodeoutlet, idtrans, payment.noinvoice, payment.subtotal, payment.diskonAmt, payment.discount, payment.serviceAmt, payment.service, payment.taxAmt, payment.tax, payment.total, payment.kembalian, payment.totalBayar, payment.userin, moment(payment.jamin).format("YYYY-MM-DD HH:mm:ss"), payment.sessionId], err => {
          if (err) return reject(err)
          const paymentItem = payment.item.map(el2 => {
            return new Promise((resolve, reject) => {
              const barang = listBarang.find(el3 => el3.kodebarang.toUpperCase() === el2.kodebarang.toUpperCase())
              const kodekategori = barang && barang.kodekategori ? barang.kodekategori : null
              const kategori = kodekategori ? listKategori.find(el3 => el3.kodekategori.toUpperCase() === kodekategori.toUpperCase()) : null
              const namakategori = kategori && kategori.namakategori ? kategori.namakategori : null
              conn.query("INSERT INTO tblorderanpaymentitem (kodeoutlet,idtrans,noinvoice,urutpayment,kodesubkategori,namasubkategori, kodebarang,namabarang,qty,discountPercent, discountAmount,harga,jumlah) VALUE (?,?,?,1,?,?,?,?,?,?,?,?,?)", [kodeoutlet, idtrans, payment.noinvoice, kodekategori, namakategori, el2.kodebarang, el2.namabarang, el2.qty, el2.discountPercent, el2.discountAmount, el2.harga, el2.jumlah], (err) => {
                if (err) return reject(err)
                return resolve()
              })
            })
          })
          const paymentDetail = new Promise((resolve, reject) => {
            const isCard = payment.paymentMethod.toUpperCase() !== 'CASH'
            conn.query("INSERT INTO tblorderanpaymentdetail (kodeoutlet,idtrans,noinvoice,urutpayment,kodepayment,namapayment,amount,isCard,isCashlez) VALUE (?,?,?,1,?,?,?,?,0)", [kodeoutlet, idtrans, payment.noinvoice, payment.kodepayment, payment.namapayment, payment.total, isCard], err => {
              if (err) return reject(err)
              return resolve()
            })
          })
          Promise.all([
            ...paymentItem,
            paymentDetail
          ]).then(() => {
            return resolve()
          }).catch(reject)
        })
      })
      hasil.then(() => {
        return resolve()
      }).catch(reject)
    } else return resolve()
  })
}

const processItemRetail = (conn, kodeoutlet, detail, idtrans, noinvoice, listKategori, listBarang) => {
  return new Promise((resolve, reject) => {
    if (detail) {
      const item = detail.map(el => {
        const barang = listBarang.find(el2 => el2.kodebarang.toUpperCase() === el.kodebarang.toUpperCase())
        const kodekategori = barang && barang.kodekategori ? barang.kodekategori : null
        const kategori = kodekategori ? listKategori.find(el2 => el2.kodekategori.toUpperCase() === kodekategori.toUpperCase()) : null
        const namakategori = kategori && kategori.namakategori ? kategori.namakategori : null
        conn.query("INSERT INTO tblorderand (kodeoutlet,idtrans,noinvoice,kodesubkategori,namasubkategori,kodebarang,namabarang,qty,discountPercent,discountAmount, harga,jumlah) VALUE (?,?,?,?,?,?,?,?,?,?,?,?)", [kodeoutlet, idtrans, noinvoice, kodekategori, namakategori, el.kodebarang, el.namabarang, el.qty, el.discountPercent, el.discountAmount, el.harga, el.jumlah], err => {
          if (err) return reject(err)
          return resolve()
        })
      })
      Promise.all(item).then(() => {
        return resolve()
      })
    } else return resolve()
  })
}

const processOrderanRetail = (conn, kodeoutlet, idtrans, noinvoice, tanggal, userin, userupt, jamin, jamupt, lastJamBayar, statusid, voidReason, userRetur) => {
  return new Promise((resolve, reject) => {
    const _lastJamBayar = lastJamBayar == null ? null : moment(lastJamBayar).format("YYYY-MM-DD HH:mm:ss")
    conn.query("INSERT INTO tblorderan (kodeoutlet,idtrans,noinvoice,tanggal,userin,userupt,jamin,jamupt,lastJamBayar,statusid,voidReason,userRetur, jenisOutlet) VALUE (?,?,?,?,?,?,?,?,?,?,?,?,'SIMPLE')", [kodeoutlet, idtrans, noinvoice, moment(tanggal).format("YYYY-MM-DD HH:mm:ss"), userin, userupt, moment(jamin).format("YYYY-MM-DD HH:mm:ss"), moment(jamupt).format('YYYY-MM-DD HH:mm:ss'), _lastJamBayar, statusid, voidReason, userRetur], (err) => {
      if (err) return reject(err)
      return resolve()
    })
  })
}

const processTransaksiRetail = (listTransaksi, kodeoutlet, listKategori, listBarang) => {
  return new Promise((resolve, reject) => {
    if (listTransaksi.length > 0) {
      db.getConnection((err, conn) => {
        if (err) return reject(err)
        conn.beginTransaction((err) => {
          if (err) {
            conn.release()
            return reject(err)
          }
          conn.config.timeout = 0
          hapusPreviousTrans(conn, kodeoutlet, listTransaksi.map(el => el._id)).then(() => {
            const hasilProcess = listTransaksi.map(el => {
              return new Promise((resolve, reject) => {
                const idtrans = el._id
                processOrderanRetail(conn, kodeoutlet, idtrans, el.noinvoice, el.tanggal, el.userin, el.userupt, el.jamin, el.jamupt, el.lastJamBayar, el.statusid, el.voidReason, el.userRetur).then(() => {
                  return processItemRetail(conn, kodeoutlet, el.detail, idtrans, el.noinvoice, listKategori, listBarang)
                }).then(() => {
                  return processPaymentRetail(conn, kodeoutlet, { paymentMethod: el.paymentMethod, kodepayment: (el.paymentMethod.toUpperCase() === "CASH" || el.paymentMethod.toUpperCase() === "TUNAI") ? "TUNAI" : el.kodepayment != null ? el.kodepayment : "NON TUNAI" /**el.kodepayment**/, namapayment: el.namapayment != null ? el.namapayment : (el.paymentMethod.toUpperCase() === "CASH" || el.paymentMethod.toUpperCase() === "TUNAI"), "TUNAI": "CREDIT" /**el.namapayment */, tax: el.tax, discount: el.discount, service: el.service, subtotal: el.subtotal, diskonAmt: el.diskonAmt, serviceAmt: el.serviceAmt, taxAmt: el.taxAmt, total: el.total, totalBayar: el.totalBayar, kembalian: el.kembalian, item: el.detail, noinvoice: el.noinvoice, sessionId: el.sessionId, userin: el.userin, jamin: el.jamin }, idtrans, listBarang, listKategori)
                }).then(() => {
                  return resolve()
                }).catch(err => {
                  console.log("Error", err)
                  const fs = require('fs')
                  fs.writeFile("D:\\error.txt", JSON.stringify(err), err2 => {
                    if (err2) console.log(err2)
                  })
                  return reject(err)
                })
              })
            })
            return Promise.all(hasilProcess)
          }).then(() => {
            conn.commit(() => {
              conn.release()
              return resolve()
            })
          }).catch(err => {
            console.log(err)
            conn.rollback(() => {
              conn.release()
              return reject(err)
            })
          })
        })
      })
    } else return resolve()
  })
}

const processRetail = (headofficecode, database, kodeoutlet, namaoutlet, userdb, pwddb) => {
  return new Promise(async (resolve, reject) => {
    const last_seq = await getLastSequence(headofficecode, database);
    const cdb = new PouchDB(`${process.env.couchdbtarget}/${database}`, {
      auth: {
        username: userdb,
        password: pwddb
      }
    })
    try {
      await checkFilter(database, cdb)
    } catch (err) {
      cdb.close()
      console.warn("Error:", err)
      return reject(err)
    }
    const host = process.env.couchdbsource || 'db.monkeypos.com'
    fetch(`https://${userdb}:${pwddb}@${host}/${database}/_changes?include_docs=true&filter=filterc/crawlfilter&since=${last_seq || 0}`).then(async resp => {
      const body = await resp.text();
      try {
        return JSON.parse(body)
      } catch (err) {
        console.warn("Error:", err)
        console.warn("Body:", body)
        return reject(err)
      }
    }).then(async resp => {
      if (resp) {
        console.log(`Processing ${resp.results.length} of data for ${database}`);
        let listKategori, listBarang;
        try {
          listKategori = await getListKategoriRetail(cdb);
          listBarang = await getListBarangRetail(cdb);
        } catch (err) {
          cdb.close()
          return reject(err)
        }
        const listTransaksi = resp.results.map(el => {
          return {
            ...el.doc
          }
        }).filter(el => el.tipe === 'transaksi')
        try {
          await processTransaksiRetail(listTransaksi, kodeoutlet, listKategori, listBarang)
          await updateLastSequence(resp, headofficecode, database)
        } catch (err) {
          console.log(err)
          cdb.close()
          return reject(err)
        }
        console.log(`${listTransaksi.length} transaksi terproses`);
        const listSession = resp.results.map(el => {
          return {
            ...el.doc
          }
        }).filter(el => el.tipe === "session")
        try {
          await processSession(listSession, kodeoutlet);
        } catch (err) {
          console.log(err)
          cdb.close()
          return (reject(err))
        }
        console.log(`${listSession.length} sesi terproses`);
        cdb.close();
        return (resolve());
      } else {
        return (reject(new Error("No Data Found")))
      }
    })
  })
}

const processByNama = (dbname) => {
  return new Promise((resolve, reject) => {
    console.log("Process by Nama", dbname)
    db.getConnection((err, conn) => {
      if (err) return (reject(err))
      conn.query("SELECT headofficecode,database,kodeoutlet,namaoutlet,userdb,pwddb,useIposStock FROM tbloutlet WHERE status <> 8 AND `database` = ?", [dbname], (err, results) => {
        conn.release()
        if (err) return (reject(err))
        if (results && results.length > 0) {
          const [outlet] = results;
          switch (outlet.tipeOutlet) {
            case "NORMAL":
              console.log(`Processing data resto for outlet ${outlet.namaoutlet}`)
              processOutlet(outlet.headofficecode, outlet.database, outlet.kodeoutlet, outlet.namaoutlet, outlet.userdb, outlet.pwddb, outlet.useIposStock).then(() => {
                console.log(`Finish processed data for outlet ${outlet.namaoutlet}`)
                return (resolve())
              }).catch(err => reject(err))
              break;
            case "SIMPLE":
              console.log(`Processing data retail for outlet ${outlet.namaoutlet}`)
              processRetail(outlet.headofficecode, outlet.database, outlet.kodeoutlet, outlet.namaoutlet, outlet.userdb, outlet.pwddb).then(() => {
                console.log(`Finish processed data for outlet ${outlet.namaoutlet}`)
                return resolve()
              }).catch(err => reject(err))
              break;
            default:
              console.log("Not Known Tipe Outlet")
              return resolve()
          }
        } else {
          console.log(`No outlet found`)
          return (resolve())
        }
      })
    })
  })
}

const processByIdtrans = (kodeoutlet, idtrans) => {
  return new Promise((resolve, reject) => {
    db.getConnection((err, conn) => {
      if (err) return reject(err)
      conn.query("SELECT * FROM tbloutlet WHERE status <> 8 AND kodeoutlet = ? AND tipeOutlet = 'NORMAL'", [kodeoutlet], (err, results) => {
        conn.release();
        if (err) return reject(err)
        if (results && results.length > 0) {
          const [outlet] = results;
          const cdb = new PouchDB(`${process.env.couchdbtarget}/${outlet.database}`, {
            auth: {
              username: outlet.userdb,
              password: outlet.pwddb
            }
          });
          Promise.all([
            getListPaymentOutlet(cdb),
            getListKategori(cdb),
            getListSubKategori(cdb),
            getListBarang(cdb),
            getListTopping(cdb)
          ]).then(([listDataPayment, listKategori, listSubkategori, listBarang, listTopping]) => {
            return cdb.get(idtrans).then(data => {
              return processTransaksi([data], kodeoutlet, listKategori, listSubkategori, listBarang, listDataPayment, listTopping, cdb)
            })
          }).then(() => {
            cdb.close();
            return resolve()
          }).catch(err => {
            cdb.close();
            return reject(err)
          })
        } else {
          console.log("No outlet found")
          return resolve()
        }
      })
    })
  })
}

const processByTanggal = (kodeoutlet, tanggal) => {
  return new Promise((resolve, reject) => {
    db.getConnection((err, conn) => {
      if (err) return reject(err)
      conn.query("SELECT * FROM tbloutlet WHERE status <> 8 AND kodeoutlet = ? AND tipeOutlet = 'NORMAL'", [kodeoutlet], (err, results) => {
        conn.release();
        if (err) return reject(err)
        if (results && results.length > 0) {
          const [outlet] = results;
          const cdb = new PouchDB(`${process.env.couchdbtarget}/${outlet.database}`, {
            auth: {
              username: outlet.userdb,
              password: outlet.pwddb
            }
          });
          Promise.all([
            getListPaymentOutlet(cdb),
            getListKategori(cdb),
            getListSubKategori(cdb),
            getListBarang(cdb),
            getListTopping(cdb)
          ]).then(([listDataPayment, listKategori, listSubkategori, listBarang, listTopping]) => {
            return cdb.find({
              selector: {
                _id: {
                  $gt: null
                },
                tipe: "transaksi",
                $and: [
                  {
                    tanggal: {
                      $gte: moment(`${tanggal} 00:00:00`, 'YYYY-MM-DD HH-mm-ss').toISOString()
                    }
                  }, {
                    tanggal: {
                      $lte: moment(`${tanggal} 23:59:59`, 'YYYY-MM-DD HH:mm:ss').toISOString()
                    }
                  }
                ]
              },
              limit: 99999
            }).then(({ docs }) => {
              return processTransaksi(docs, kodeoutlet, listKategori, listSubkategori, listBarang, listDataPayment, listTopping, cdb)
            })
          }).then(() => {
            cdb.close()
            return resolve()
          }).catch(err => {
            cdb.close();
            return reject(err)
          })
        } else {
          console.log("No outlet found")
          return resolve()
        }
      })
    })
  })
}

const processKasByIdTrans = (kodeoutlet, idtrans) => {
  return new Promise((resolve, reject) => {
    db.getConnection((err, conn) => {
      if (err) return reject(err)
      conn.query("SELECT * FROM tbloutlet WHERE status <> 8 AND kodeoutlet = ? AND tipeOutlet = 'NORMAL'", [kodeoutlet], (err, results) => {
        conn.release();
        if (err) return reject(err)
        if (results && results.length > 0) {
          const [outlet] = results;
          const cdb = new PouchDB(`${process.env.couchdbtarget}/${outlet.database}`, {
            auth: {
              username: outlet.userdb,
              password: outlet.pwddb
            }
          });
          cdb.get(idtrans).then(data => {
            if (idtrans.match(/^transkasmasuk-/)) {
              return processKasMasuk([data], kodeoutlet)
            } else if (idtrans.match(/^transkaskeluar-/)) {
              return processKasKeluar([data], kodeoutlet)
            } else return null
          }).then(() => {
            cdb.close()
            return resolve()
          }).catch(err => {
            cdb.close();
            return reject(err)
          })
        } else {
          console.log("No outlet found")
          return resolve()
        }
      })
    })
  })
}

const processKasByTanggal = (kodeoutlet, tanggal) => {
  return new Promise((resolve, reject) => {
    db.getConnection((err, conn) => {
      if (err) return reject(err)
      conn.query("SELECT * FROM tbloutlet WHERE status <> 8 AND kodeoutlet = ? AND tipeOutlet = 'NORMAL'", [kodeoutlet], (err, results) => {
        conn.release();
        if (err) return reject(err)
        if (results && results.length > 0) {
          const [outlet] = results;
          const cdb = new PouchDB(`${process.env.couchdbtarget}/${outlet.database}`, {
            auth: {
              username: outlet.userdb,
              password: outlet.pwddb
            }
          });
          cdb.find({
            selector: {
              _id: {
                $gt: null
              },
              tipe: {
                $gt: null
              },
              $or: [
                {
                  tipe: 'transkasmasuk'
                },
                {
                  tipe: "transkaskeluar"
                }
              ],
              $and: [
                {
                  tanggal: {
                    $gte: moment(`${tanggal} 00:00:00`, 'YYYY-MM-DD HH-mm-ss').toISOString()
                  }
                }, {
                  tanggal: {
                    $lte: moment(`${tanggal} 23:59:59`, 'YYYY-MM-DD HH:mm:ss').toISOString()
                  }
                }
              ]
            },
            limit: 99999
          }).then(({ docs }) => {
            const data = []
            docs.reduce((res, value) => {
              if (!res[value.tipe]) {
                res[value.tipe] = {
                  tipe: value.tipe,
                  data: []
                }
                data.push(res[value.tipe])
              }
              res[value.tipe].data.push(value)
              return res;
            }, {})
            const hasilProcess = data.map(el => {
              if (el.tipe === 'transkasmasuk') {
                return processKasMasuk(el.data, kodeoutlet)
              } else if (el.tipe === 'transkaskeluar') {
                return processKasKeluar(el.data, kodeoutlet)
              } else {
                return Promise.resolve()
              }
            })
            return Promise.all(hasilProcess)
          }).then(() => {
            cdb.close()
            return resolve()
          }).catch(err => {
            cdb.close();
            return reject(err)
          })
        } else {
          console.log("No outlet found")
          return resolve()
        }
      })
    })
  })
}

exports.outlet = processByNama
exports.idtrans = processByIdtrans
exports.tanggal = processByTanggal
exports.kas = processKasByIdTrans
exports.kastanggal = processKasByTanggal