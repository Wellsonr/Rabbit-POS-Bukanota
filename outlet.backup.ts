/* eslint-disable no-async-promise-executor */
import './env';
import db from './dbconnection';
import moment from 'moment-timezone';
import PouchDB from 'pouchdb';
import PouchDBFind from 'pouchdb-find';
import fetch from 'node-fetch';
import Cosinesimilarity from './Cosinesimilarity';
import { Connection } from 'mysql';
import cmp from 'semver-compare';
import dbmid from './dbconnectionmid'
import { sha256 } from 'js-sha256';

PouchDB.plugin(PouchDBFind);

interface MysqlInfo {
  user: string,
  password: string,
  host: string,
  dbname: string,
}

interface Session {
  _id: string
  _rev: string
  sessionId: string
  saldoAwal: number
  tanggalBuka: string
  userBuka: string
  tipe: string
  version?: string
  userTutup?: string
  tanggalTutup?: string
  saldoAkhir?: number
  [others: string]: any
}

interface KasDetail {
  kodebiaya: string
  namabiaya: string
  amount: number
  keterangan?: string
}

interface TransaksiKas {
  _id: string
  _rev: string
  tanggal: string
  detail: KasDetail[]
  userin: string
  jamin: string
  tipe: string
  noinvoice: string
  sessionId: string
  [others: string]: any
}
interface TransaksiTopping {
  kodetopping: string
  namatopping: string
  qty: number
  harga: number
  jumlah: number
  [others: string]: any
}

interface CaptainOrderItem {
  kodebarang: string
  namabarang: string
  qty: number
  listOrderTopping?: TransaksiTopping[]
  [others: string]: any
}
interface CaptainOrder {
  item: CaptainOrderItem[]
  kodewaiter?: string | null
  userin: string
  jamin: string
  [others: string]: any
}

interface TransaksiVariant {
  kodebarang: string
  namabarang: string
  kodevariant: string
  namavariant: string
  harga: number
  [others: string]: any
}

interface TransaksiDetail {
  kodesubkategori: string
  kodebarang: string
  namabarang: string
  qty: number
  discountPercent: number
  discountAmount: number
  harga: number
  jumlah: number
  isPromo?: boolean
  index: number
  listOrderTopping?: TransaksiTopping[]
  isVariant?: boolean
  variant?: TransaksiVariant
  qtyVoid?: number
  [others: string]: any
}

interface TransaksiPaymentItem {
  kodesubkategori: string
  kodebarang: string
  namabarang: string
  qty: number
  harga: number
  discountPercent: number
  discountAmount: number
  jumlah: number
  index: number
  listOrderTopping?: TransaksiTopping[]
  isVariant?: boolean
  variant?: TransaksiVariant
  qtyVoid?: number
  [others: string]: any
}

interface TransaksiPaymentDetail {
  kodepayment: string
  namapayment: string
  remark?: string | null
  nomorKartu?: string | null
  transRef?: string | null
  amount: number
  [others: string]: any
}

interface TransaksiPayment {
  noinvoice: string
  item: TransaksiPaymentItem[]
  payment: TransaksiPaymentDetail[]
  subtotal: number
  promoAmount: number
  discountAmount: number
  discountPercent: number
  serviceAmount: number
  servicePercent: number
  taxAmount: number
  taxPercent: number
  ongkir: number
  total: number
  pembulatan: number
  kembalian: number
  totalPayment: number
  userin: string
  jamin: string
  sessionId: string
  isPromo?: boolean
  kodePromo?: string
  namaPromo?: string
  [others: string]: any
}

interface Transaksi {
  _id: string
  _rev: string
  tanggal: string
  nomeja: string
  nomejaint: number
  namacustomer: string
  keterangan: string
  detail: TransaksiDetail[]
  captainOrder: CaptainOrder[]
  userin: string
  userupt: string
  jamin: string
  displayHarga: string
  cover?: number
  tipe: string
  useDelivery?: boolean
  version?: string
  jamupt: string
  payment: TransaksiPayment[]
  statusid: number
  lastJamBayar?: string
  noinvoice?: string
  noretur?: string
  userRetur?: string
  sessionRetur?: string
  jamretur?: string
  voidReason?: string
  [others: string]: any
}
interface PaymentOutlet {
  _id?: string,
  _rev?: string,
  kodepayment: string,
  namapayment: string,
  userin: string,
  userupt: string,
  jamin: string,
  jamupt: string,
  isAutoFill?: boolean,
  isCard?: boolean,
  isPassti?: boolean,
  isQREN?: boolean,
  isSystem?: boolean,
  tipe: string,
  [others: string]: any
}

interface Kategori {
  _id: string,
  _rev: string,
  kodekategori: string,
  namakategori: string,
  userin: string,
  userupt: string,
  jamin: string,
  jamupt: string,
  tipe: string,
  isHide?: boolean,
  [others: string]: any
}

interface Topping {
  _id: string,
  _rev: string,
  kodetopping: string,
  namatopping: string,
  hargasatuan: number,
  hargagojek: number,
  hargagrab: number,
  hargalain: number,
  tipe: string,
  userin: string,
  userupt: string,
  jamin: string,
  jamupt: string,
  kodekategori: string,
  kodesubkategori: string,
  [others: string]: any
}

interface SubKategori {
  _id: string,
  _rev: string,
  kodesubkategori: string,
  namasubkategori: string,
  tipe: string,
  userin: string,
  userupt: string,
  jamin: string,
  jamupt: string,
  [others: string]: any
}

interface Variant {
  kodebarang: string,
  namabarang: string,
  hargasatuan: number,
  hargagojek: number,
  hargagrab: number,
  hargalain: number,
  [others: string]: any
}
interface Barang {
  _id: string,
  _rev: string,
  kodebarang: string,
  namabarang: string,
  kodekategori: string,
  kodesubkategori: string,
  satuan: string,
  hargasatuan: number,
  hargagojek: number,
  hargagrab: number,
  hargalain: number,
  tipe: string,
  userin: string,
  userupt: string,
  jamin: string,
  jamupt: string,
  printer: string,
  topping: string,
  variant: Variant[],
  [others: string]: any
}

const isNumeric = (n: any): Boolean => !isNaN(parseFloat(n)) && isFinite(n);

const getListPaymentOutlet = (cdb) => {
  return new Promise<PaymentOutlet[]>((resolve, reject) => {
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
  return new Promise<any[]>((resolve, reject) => {
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
  return new Promise<Kategori[]>((resolve, reject) => {
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
  return new Promise<Topping[]>((resolve, reject) => {
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
  return new Promise<SubKategori[]>((resolve, reject) => {
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
  return new Promise<any[]>((resolve, reject) => {
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
  return new Promise<Barang[]>((resolve, reject) => {
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

const getLastSequence = (headofficecode: string, database: string) => {
  return new Promise<string | null>((resolve, reject) => {
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

const checkFilter = (database: string, cdb: any): Promise<any> => {
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

const updateLastSequence = (resp: any, headofficecode: string, database: string, last_seq: string) => {
  return new Promise<void>((resolve, reject) => {
    db.getConnection((err, conn) => {
      if (err) return (reject(err));
      conn.beginTransaction((err) => {
        if (err) {
          return reject(err);
        }
        const promInsert = new Promise<void>((resolve1, reject1) => {
          conn.query("INSERT INTO tblsequence (headofficecode,`database`,lastSequence) VALUE (?,?,?)", [headofficecode, database, resp.last_seq], err => {
            if (err) return reject1(err);
            return resolve1()
          })
        });
        promInsert.then(() => {
          const promDelete = new Promise<void>((resolve1, reject1) => {
            conn.query("SELECT @id := urut FROM tblsequence WHERE headofficecode = ? AND `database` = ? AND lastSequence = ? LIMIT 1; DELETE FROM tblsequence WHERE headofficecode = ? AND `database` = ? AND urut < @id;", [headofficecode, database, last_seq, headofficecode, database], (err) => {
              if (err) {
                return reject1(err);
              } else {
                return resolve1();
              }
            });
          });
          return promDelete;
        }).then(() => {
          conn.commit(() => {
            conn.release();
            return resolve();
          });
        }).catch(err => {
          conn.rollback(() => {
            conn.release();
            return reject(err);
          });
        });
      });
    })
  })
}

const hapusPreviousTrans = (conn: Connection, kodeoutlet: string, idtrans: string[]) => {
  return new Promise<void>((resolve, reject) => {
    const banyakTrans = idtrans.length
    const jumlahLoop = Math.ceil(banyakTrans / 1000)
    const listPromise = []
    for (let i = 0; i < jumlahLoop; i++) {
      const split = idtrans.slice((i * 1000), (i * 1000) + 999)
      // console.log("HASIL SPLIT", i, split)
      listPromise.push(new Promise<void>((resolve, reject) => {
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

const processOrderan = (conn: Connection, kodeoutlet: string, idtrans: string, noinvoice: string, tanggal: string, nomeja: string, namacustomer: string, keterangan: string, userin: string, userupt: string, jamin: string, jamupt: string, lastJamBayar: string | null, statusid: number, displayHarga: string, cover: number, voidReason: string, userRetur: string, useDelivery: boolean, member: {
  nama?: string
  alamat?: string
  nohandphone?: string
  zona?: string
} | null) => {
  return new Promise<void>((resolve, reject) => {
    const _lastJamBayar: string | null = lastJamBayar == null ? null : moment(lastJamBayar).format("YYYY-MM-DD HH:mm:ss")
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

const processCaptainOrder = (conn: Connection, kodeoutlet: string, captainOrder: CaptainOrder[], idtrans: string, noinvoice: string, detail: TransaksiDetail[], listSubkategori: SubKategori[], listBarang: Barang[], listTopping: Topping[]) => {
  return new Promise<void>((resolve, reject) => {
    if (captainOrder) {
      const CO = captainOrder.map((el, index) => {
        return new Promise<void>((resolve, reject) => {
          conn.query("INSERT INTO tblorderanco (kodeoutlet,idtrans,noinvoice, urutco, kodewaiter, userin, jamin) VALUE (?,?,?,?,?,?,?)", [kodeoutlet, idtrans, noinvoice, index + 1, el.kodewaiter, el.userin, moment(el.jamin).format("YYYY-MM-DD HH:mm:ss")], (err) => {
            if (err) return (reject(err))
            const listDetail = el.item.map(el2 => {
              return new Promise<void>((resolve, reject) => {
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
                      return new Promise<void>((resolve, reject) => {
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
                    Promise.all<void>(topping).then(() => resolve())
                  } else return (resolve())
                })
              })
            })
            let listItemPaket = []
            for (let i = 0, j = el.item.length; i < j; i++) {
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
              return new Promise<void>((resolve, reject) => {
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
                      return new Promise<void>((resolve, reject) => {
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
                    Promise.all<void>(topping).then(() => resolve())
                  } else return (resolve())
                })
              })
            })
            Promise.all<void>([
              ...listDetail,
              ...listPaket
            ]).then(() => {
              return (resolve())
            }).catch(err => {
              return (reject(err))
            })
          })
        })
      })
      Promise.all<void>(CO).then(() => {
        return (resolve())
      }).catch(err => {
        return (reject(err))
      })
    } else {
      return resolve();
    }
  })
}

const processItem = (conn: Connection, kodeoutlet: string, detail: TransaksiDetail[], idtrans: string, noinvoice: string, listKategori: Kategori[], listSubkategori: SubKategori[], listBarang: Barang[], listTopping: Topping[]) => {
  return new Promise<void>((resolve, reject) => {
    if (detail) {
      const item = detail.map(el => {
        return new Promise<void>((resolve, reject) => {
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
          const qtyVoid = el.qtyVoid != null ? el.qtyVoid : 0;
          conn.query("INSERT INTO tblorderand (kodeoutlet, idtrans, noinvoice, kodekategori, namakategori, kodesubkategori, namasubkategori, kodebarang, namabarang, qty, discountPercent, discountAmount, harga, jumlah, isPaket, isPromo, `index`, isVariant, isTopping, qtyVoid) VALUE (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,FALSE,?)", [kodeoutlet, idtrans, noinvoice, kodekategori, namakategori, kodesubkategori, namasubkategori, el.kodebarang, el.namabarang, el.qty, el.discountPercent, el.discountAmount, el.harga, el.jumlah, isPaket, isPromo, el.index, isVariant, qtyVoid], (err) => {
            if (err) return reject(err)
            const listOrderTopping = el.listOrderTopping || [];
            if (listOrderTopping.length > 0) {
              const topping = listOrderTopping.map(top => {
                return new Promise<void>((resolve, reject) => {
                  const topping = listTopping.find(tp => tp.kodetopping.toUpperCase() === top.kodetopping.toUpperCase())
                  const kodekategoritop = topping && topping.kodekategori ? topping.kodekategori : null
                  const kategoritop = kodekategoritop ? listKategori.find(el2 => el2.kodekategori.toUpperCase() === kodekategoritop.toUpperCase()) : null
                  const namakategoritop = kategoritop && kategoritop.namakategori ? kategoritop.namakategori : null
                  const kodesubkategoritop = topping && topping.kodesubkategori ? topping.kodesubkategori : null
                  const subkategoritop = kodesubkategoritop ? listSubkategori.find(el3 => el3.kodesubkategori.toUpperCase() === kodesubkategoritop.toUpperCase()) : null
                  const namasubkategoritop = subkategoritop && subkategoritop.namasubkategori ? subkategoritop.namasubkategori : null
                  conn.query("INSERT INTO tblorderand (kodeoutlet,idtrans,noinvoice,kodekategori, namakategori, kodesubkategori, namasubkategori, kodebarang,namabarang,qty,discountPercent, discountAmount, harga, jumlah,isPaket,isPromo,`index`,isTopping,isVariant,kodebarangtopping,namabarangtopping, qtyVoid) VALUE (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", [kodeoutlet, idtrans, noinvoice, kodekategoritop, namakategoritop, kodesubkategoritop, namasubkategoritop, top.kodetopping, top.namatopping, top.qty * el.qty, 0, 0, top.harga, (el.qty * top.qty) * top.harga, 0, 0, el.index, true, false, el.kodebarang, el.namabarang, top.qty * qtyVoid], (err => {
                    if (err) return reject(err)
                    return resolve()
                  }))
                })
              })
              Promise.all<void>(topping).then(() => resolve())
            } else return (resolve())
          })
        })
      })
      let listItemPaket = []
      for (let i = 0, j = detail.length; i < j; i++) {
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
        return new Promise<void>((resolve, reject) => {
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
          const qtyVoid = el.qtyVoid != null ? el.qtyVoid : 0;
          conn.query("INSERT INTO tblorderandpaket (kodeoutlet, idtrans, noinvoice, kodekategori, namakategori, kodesubkategori, namasubkategori, kodebarang, namabarang, qty, discountPercent, discountAmount, harga, jumlah, kodepaket, isPromo, `index`, isVariant, isTopping, qtyVoid) VALUE (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,FALSE,?)", [kodeoutlet, idtrans, noinvoice, kodekategori, namakategori, kodesubkategori, namasubkategori, el.kodebarang, el.namabarang, el.qty, el.discountPercent, el.discountAmount, el.harga, el.jumlah, kodepaket, isPromo, el.index, isVariant, qtyVoid], (err) => {
            if (err) return reject(err)
            const listOrderTopping = el.listOrderTopping || []
            if (listOrderTopping.length > 0) {
              const topping = listOrderTopping.map(top => {
                return new Promise<void>((resolve, reject) => {
                  const topping = listTopping.find(tp => tp.kodetopping.toUpperCase() === top.kodetopping.toUpperCase())
                  const kodekategoritop = topping && topping.kodekategori ? topping.kodekategori : null
                  const kategoritop = kodekategoritop ? listKategori.find(el2 => el2.kodekategori.toUpperCase() === kodekategoritop.toUpperCase()) : null
                  const namakategoritop = kategoritop && kategoritop.namakategori ? kategoritop.namakategori : null
                  const kodesubkategoritop = topping && topping.kodesubkategori ? topping.kodesubkategori : null
                  const subkategoritop = kodesubkategoritop ? listSubkategori.find(el3 => el3.kodesubkategori.toUpperCase() === kodesubkategoritop.toUpperCase()) : null
                  const namasubkategoritop = subkategoritop && subkategoritop.namasubkategori ? subkategoritop.namasubkategori : null
                  conn.query("INSERT INTO tblorderandpaket (kodeoutlet, idtrans, noinvoice, kodekategori, namakategori, kodesubkategori, namasubkategori, kodebarang, namabarang, qty, discountPercent, discountAmount, harga, jumlah, kodepaket, isPromo, `index`, isVariant, isTopping,kodebarangtopping,namabarangtopping, qtyVoid) VALUE (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", [kodeoutlet, idtrans, noinvoice, kodekategoritop, namakategoritop, kodesubkategoritop, namasubkategoritop, top.kodetopping, top.namatopping, top.qty * el.qty, 0, 0, top.harga, (el.qty * top.qty) * top.harga, kodepaket, 0, el.index, false, true, el.kodebarang, el.namabarang, top.qty * qtyVoid], (err => {
                    if (err) return reject(err)
                    return resolve()
                  }))
                })
              })
              Promise.all<void>(topping).then(() => resolve())
            } else return (resolve())
          })
        })
      })
      Promise.all<void>([
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

const processPayment = (conn: Connection, kodeoutlet: string, payment: TransaksiPayment[], idtrans: string, noinvoice: string, listPayment: PaymentOutlet[], listBarang: Barang[], listSubkategori: SubKategori[], listTopping: Topping[]) => {
  return new Promise<void>((resolve, reject) => {
    if (payment) {
      const hasil = payment.map((el, index) => {
        return new Promise<void>((resolve, reject) => {
          const promoAmount = el.promoAmount != null ? el.promoAmount : 0
          conn.query("INSERT INTO tblorderanpayment (kodeoutlet,idtrans,noinvoice, urutpayment, subtotal, discountAmount, discountPercent, serviceAmount, servicePercent, taxAmount, taxPercent, total, pembulatan, kembalian, totalPayment, userin, jamin, sessionId,promoAmount, kodePromo,namaPromo,ongkir) VALUE (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", [kodeoutlet, idtrans, noinvoice, index + 1, el.subtotal, el.discountAmount, el.discountPercent, el.serviceAmount, el.servicePercent, el.taxAmount, el.taxPercent, el.total, el.pembulatan, el.kembalian, el.totalPayment, el.userin, moment(el.jamin).format("YYYY-MM-DD HH:mm:ss"), el.sessionId, promoAmount, el.kodePromo, el.namaPromo, el.ongkir], err => {
            if (err) return reject(err)
            const paymentItem = el.item.map(el2 => {
              return new Promise<void>((resolve, reject) => {
                const barang = listBarang.find(el3 => el3.kodebarang.toUpperCase() === el2.kodebarang.toUpperCase() || (el3.variant || []).map(el4 => el4.kodebarang.toUpperCase()).indexOf(el2.kodebarang.toUpperCase()) > -1)
                const kodesubkategori = el2.kodesubkatgori ? el2.kodesubkategori : barang && barang.kodesubkategori ? barang.kodesubkategori : null
                const subkategori = kodesubkategori ? listSubkategori.find(el3 => el3.kodesubkategori.toUpperCase() === kodesubkategori.toUpperCase()) : null
                const namasubkategori = subkategori && subkategori.namasubkategori ? subkategori.namasubkategori : null
                const isPromo = el2.isPromo != null ? el2.isPromo : false
                const isVariant = el2.isVariant != null ? el2.isVariant : false
                const qtyVoid = el2.qtyVoid != null ? el2.qtyVoid : 0;
                conn.query("INSERT INTO tblorderanpaymentitem (kodeoutlet,idtrans,noinvoice,urutpayment,kodesubkategori, namasubkategori, kodebarang,namabarang,qty,discountPercent,discountAmount,harga,jumlah,isPromo,`index`, isTopping,isVariant, qtyVoid) VALUE (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,FALSE,?,?)", [kodeoutlet, idtrans, noinvoice, index + 1, kodesubkategori, namasubkategori, el2.kodebarang, el2.namabarang, el2.qty, el2.discountPercent, el2.discountAmount, el2.harga, el2.jumlah, isPromo, el2.index, isVariant, qtyVoid], err => {
                  if (err) return reject(err)
                  const listOrderTopping = el2.listOrderTopping || []
                  if (listOrderTopping.length > 0) {
                    const topping = listOrderTopping.map(top => {
                      return new Promise<void>((resolve, reject) => {
                        const topping = listTopping.find(tp => tp.kodetopping.toUpperCase() === top.kodetopping.toUpperCase())
                        const kodesubkategoritop = topping && topping.kodesubkategori ? topping.kodesubkategori : null
                        const subkategoritop = kodesubkategoritop ? listSubkategori.find(el3 => el3.kodesubkategori.toUpperCase() === kodesubkategoritop.toUpperCase()) : null
                        const namasubkategoritop = subkategoritop && subkategoritop.namasubkategori ? subkategoritop.namasubkategori : null
                        conn.query("INSERT INTO tblorderanpaymentitem (kodeoutlet,idtrans,noinvoice,urutpayment,kodesubkategori,namasubkategori,kodebarang,namabarang,qty,discountPercent,discountAmount,harga,jumlah,isPromo,`index`,isTopping,isVariant,kodebarangtopping,namabarangtopping, qtyVoid) VALUE (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", [kodeoutlet, idtrans, noinvoice, index + 1, kodesubkategoritop, namasubkategoritop, top.kodetopping, top.namatopping, top.qty * el2.qty, 0, 0, top.harga, (el2.qty * top.qty) * top.harga, 0, el2.index, true, false, el2.kodebarang, el2.namabarang, top.qty * qtyVoid], (err) => {
                          if (err) return reject(err)
                          return resolve()
                        })
                      })
                    })
                    Promise.all<void>(topping).then(() => resolve())
                  } else return (resolve())
                })
              })
            })
            const paymentDetail = el.payment.map((el2) => {
              return new Promise<void>((resolve, reject) => {
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
            Promise.all<void>([
              ...paymentItem,
              ...paymentDetail
            ]).then(() => {
              return resolve()
            }).catch(reject)
          })
        })
      })
      Promise.all<void>(hasil).then(() => {
        return (resolve())
      }).catch(reject)
    } else {
      return resolve()
    }
  })
}

const convertPhone = (val: string) => {
  let handphone = val
  if (val.match(/^\+62/)) handphone = val.replace(/^\+62/, '0')
  if (val.match(/^\+65/)) handphone = val.replace(/^\+62/, '')
  return handphone.trim()
}

const groupBy = function (xs: any, key: any) {
  return xs.reduce(function (rv, x) {
    (rv[x[key]] = rv[x[key]] || []).push(x);
    return rv;
  }, {});
};

const processMember = (members: any[], cdb: any) => {
  // eslint-disable-next-line no-async-promise-executor
  return new Promise<void>(async (resolve, reject) => {
    const sorted = members.filter(el => el).map(el => {
      return {
        ...el,
        nohandphone: convertPhone(el.nohandphone)
      }
    })
    const groupResult = groupBy(sorted, "nohandphone")
    let hitung = 0;
    for (const hp of Object.keys(groupResult)) {
      const nohandphone = convertPhone(hp);
      if (nohandphone != '') {
        await cdb.get(`member-${nohandphone}`).then(async doc => {
          const previousAlamat = doc.dataMember.map(el => el.alamat)
          let newAlamat = [];
          for (let i = 0, j = groupResult[hp].length; i < j; i++) {
            const checkAlamat = [...previousAlamat, ...newAlamat.map(el => el.alamat)]
            let max = 0.0;
            for (let k = 0, l = checkAlamat.length; k < l; k++) {
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
            for (let i = 0, j = groupResult[hp].length; i < j; i++) {
              const checkAlamat = [...newAlamat]
              let max = 0.0;
              for (let k = 0, l = checkAlamat.length; k < l; k++) {
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

const cekDetail = (cdb: any, conn: Connection, kodeoutlet: string, transaksi: Transaksi): Promise<Transaksi> => {
  return new Promise((resolve, reject) => {
    new Promise<Transaksi>((resolveBalance, rejectBalance) => {
      if (transaksi.statusid === 20 || transaksi.statusid === 9) {
        const jumlahDetail = transaksi.detail.map(el => el.jumlah).reduce((a, b) => a + b, 0);
        const jumlahPayment = transaksi.payment.map(el => el.item.map(el2 => el2.jumlah).reduce((a, b) => a + b, 0)).reduce((a, b) => a + b, 0);
        if (jumlahDetail !== jumlahPayment) {
          let listItem: TransaksiDetail[] = [];
          for (let i = 0, j = transaksi.payment.length; i < j; i++) {
            for (let k = 0, l = transaksi.payment[i].item.length; k < l; k++) {
              const { kodebarang, index, qty, jumlah, discountAmount } = transaksi.payment[i].item[k];
              const found = listItem.find(el => el.kodebarang.toUpperCase() === kodebarang.toUpperCase() && el.index === index);
              if (found) {
                listItem = listItem.map(el => {
                  if (el.kodebarang.toUpperCase() === kodebarang.toUpperCase() && el.index === index) {
                    return {
                      ...el,
                      qty: el.qty + qty,
                      discountAmount: el.discountAmount + discountAmount,
                      jumlah: el.jumlah + jumlah,
                    }
                  } else {
                    return el;
                  }
                });
              } else {
                listItem.push(transaksi.payment[i].item[k]);
              }
            }
          }
          const detailBackup = [...transaksi.detail];
          if (transaksi._id.match(/^retur-/g)) {
            const newUpdate = {
              ...transaksi,
              detailBackup,
              detail: listItem,
            };
            //update
            cdb.put(newUpdate)
              .then(() => {
                return resolveBalance(newUpdate);
              }).catch(rejectBalance);
          } else {
            const captainOrderBackup = [...transaksi.captainOrder || []];
            const listCaptainOrder: CaptainOrder[] = [];
            if (transaksi.captainOrder[0].item.length === listItem.length) {
              listCaptainOrder.push(transaksi.captainOrder[0]);
            } else {
              //check 1 per 1
              conn.query('INSERT INTO tbldataproblem (kodeoutlet, notrans, tanggal) VALUES (?,?,?)', [kodeoutlet, transaksi._id, moment(transaksi.tanggal).format("YYYY-MM-DD HH:mm:ss")], (err) => {
                if (err) {
                  return rejectBalance(err);
                }
                return resolveBalance(transaksi);
              });
            }
            const newUpdate = {
              ...transaksi,
              detailBackup,
              detail: listItem,
              captainOrderBackup,
              captainOrder: listCaptainOrder,
            };
            //update
            cdb.put(newUpdate)
              .then(() => {
                return resolveBalance(newUpdate);
              }).catch(rejectBalance);
          }
        } else {
          return resolveBalance(transaksi);
        }
      } else {
        return resolveBalance(transaksi);
      }
    })
      .then(newTransaksi => new Promise<Transaksi>((resolvePayment, rejectPayment) => {
        let isChanged = false;
        const prosesCekPayment = i => {
          if (!isNumeric(newTransaksi.payment[i].total)) {
            isChanged = true;
            const subTotalDiscount = newTransaksi.payment[i].subtotal - newTransaksi.payment[i].discountAmount;
            newTransaksi.payment[i].serviceAmount = 0;
            newTransaksi.payment[i].servicePercent = 0;
            newTransaksi.payment[i].taxAmount = 0;
            newTransaksi.payment[i].taxPercent = 0;
            newTransaksi.payment[i].total = subTotalDiscount;
            newTransaksi.payment[i].payment[i].amount = subTotalDiscount;
            newTransaksi.payment[i].pembulatan = 0;
            newTransaksi.payment[i].kembalian = 0;
            newTransaksi.payment[i].totalPayment = subTotalDiscount;
            newTransaksi.payment[i].isPaymentError = true;
            if (i < newTransaksi.payment.length - 1) {
              i++;
              prosesCekPayment(i);
            } else {
              if (isChanged) {
                cdb
                  .put(newTransaksi)
                  .then(() => resolvePayment(newTransaksi))
                  .catch(rejectPayment);
              } else {
                return resolvePayment(newTransaksi);
              }
            }
          } else {
            if (i < newTransaksi.payment.length - 1) {
              i++;
              prosesCekPayment(i);
            } else {
              if (isChanged) {
                cdb
                  .put(newTransaksi)
                  .then(() => resolvePayment(newTransaksi))
                  .catch(rejectPayment);
              } else {
                return resolvePayment(newTransaksi);
              }
            }
          }
        }
        if (newTransaksi.statusid === 20 || transaksi.statusid === 9) {
          if (newTransaksi.payment.length > 0) {
            prosesCekPayment(0);
          } else {
            return resolvePayment(newTransaksi);
          }
        } else {
          return resolvePayment(newTransaksi);
        }
      }))
      .then(newTransaksi => resolve(newTransaksi))
      .catch(err => reject(err));
  });
}

const processTransaksi = (listTransaksi: Transaksi[], kodeoutlet: string, listKategori: Kategori[], listSubkategori: SubKategori[], listBarang: Barang[], listPayment: PaymentOutlet[], listTopping: Topping[], cdb: any) => {
  return new Promise<void>((resolve, reject) => {
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
            for (let i = 0; i < jumlahLoop; i++) {
              const split = listTransaksi.slice((i * 1000), (i * 1000) + 999)
              listPromise.push(new Promise<void>((resolve, reject) => {
                const hasilProcess = split.map(item => {
                  return new Promise<void>((resolve, reject) => {
                    cekDetail(cdb, conn, kodeoutlet, item).then(el => {
                      const idtrans = el._id
                      processOrderan(conn, kodeoutlet, idtrans, el.noinvoice, el.tanggal, el.nomeja, el.namacustomer, el.keterangan, el.userin, el.userupt, el.jamin, el.jamupt, el.lastJamBayar, el.statusid, el.displayHarga, el.cover, el.voidReason, el.userRetur, el.useDelivery, el.member).then(() => {
                        return processCaptainOrder(conn, kodeoutlet, el.captainOrder, idtrans, el.noinvoice, el.detail, listSubkategori, listBarang, listTopping)
                      }).then(() => {
                        return processItem(conn, kodeoutlet, el.detail, idtrans, el.noinvoice, listKategori, listSubkategori, listBarang, listTopping)
                      }).then(() => {
                        return processPayment(conn, kodeoutlet, el.payment, idtrans, el.noinvoice, listPayment, listBarang, listSubkategori, listTopping)
                      }).then(() => {
                        return resolve();
                      }).catch(err => {
                        console.log("Error", err)
                        // eslint-disable-next-line @typescript-eslint/no-var-requires
                        const fs = require('fs')
                        fs.writeFile("D:\\error.txt", JSON.stringify(err), (err2) => {
                          if (err2) console.log(err2)
                        })
                        return reject(err)
                      })
                    });
                  })
                })
                Promise.all<void>(hasilProcess).then(() => resolve()).catch(reject)
              }))
            }
            return Promise.all<void>(listPromise)
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

const hapusPreviousSession = (conn: Connection, kodeoutlet: string, listSession: string[]) => {
  return new Promise<void>((resolve, reject) => {
    conn.query("DELETE FROM tblsession WHERE kodeoutlet = ? AND sessionId IN (?)", [kodeoutlet, listSession], (err) => {
      if (err) return reject(err)
      return resolve();
    })
  })
}

const processSession = (listSession: Session[], kodeoutlet: string) => {
  return new Promise<void>((resolve, reject) => {
    if (listSession.length > 0) {
      db.getConnection((err, conn) => {
        if (err) return reject(err)
        conn.beginTransaction((err) => {
          if (err) return reject(err)
          hapusPreviousSession(conn, kodeoutlet, listSession.map(el => el.sessionId)).then(() => {
            const hasilProcess = listSession.map(el => {
              return new Promise<void>((resolve, reject) => {
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

const hapusPreviousKasMasuk = (conn: Connection, kodeoutlet: string, listTrans: string[]) => {
  return new Promise<void>((resolve, reject) => {
    conn.query("DELETE FROM tbltranskasmasuk WHERE kodeoutlet = ? AND idtrans IN (?)", [kodeoutlet, listTrans], (err) => {
      if (err) return reject(err)
      conn.query("DELETE FROM tbltranskasmasukd WHERE kodeoutlet = ? AND idtrans IN (?)", [kodeoutlet, listTrans], (err) => {
        if (err) return reject(err)
        return resolve()
      })
    })
  })
}

const processKasMasuk = (listKasMasuk: TransaksiKas[], kodeoutlet: string) => {
  return new Promise<void>((resolve, reject) => {
    if (listKasMasuk.length > 0) {
      db.getConnection((err, conn) => {
        if (err) return reject(err)
        conn.beginTransaction((err) => {
          if (err) return reject(err)
          hapusPreviousKasMasuk(conn, kodeoutlet, listKasMasuk.map(el => el._id)).then(() => {
            const hasilProcess = listKasMasuk.map(el => {
              return new Promise<void>((resolve, reject) => {
                const jamin = el.jamin != null ? moment(el.jamin).format("YYYY-MM-DD HH:mm:ss") : undefined;
                const tanggal = el.tanggal != null ? moment(el.tanggal).format("YYYY-MM-DD HH:mm:ss") : undefined
                conn.query("INSERT INTO tbltranskasmasuk (kodeoutlet,idtrans,noinvoice,sessionId,userin,jamin,tanggal) VALUE (?,?,?,?,?,?,?)", [kodeoutlet, el._id, el.noinvoice, el.sessionId, el.userin, jamin, tanggal], (err) => {
                  if (err) return reject(err)
                  const promises = el.detail.map(dt => {
                    return new Promise<void>((resolve, reject) => {
                      conn.query("INSERT INTO tbltranskasmasukd (kodeoutlet, idtrans, noinvoice, kodebiaya, namabiaya, amount, keterangan) VALUE (?,?,?,?,?,?,?)", [kodeoutlet, el._id, el.noinvoice, dt.kodebiaya, dt.namabiaya, dt.amount, dt.keterangan], (err) => {
                        if (err) return reject()
                        else return resolve()
                      })
                    })
                  })
                  Promise.all<void>(promises).then(() => resolve()).catch(reject)
                })
              })
            })
            return Promise.all<void>(hasilProcess)
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

const hapusPreviousKasKeluar = (conn: Connection, kodeoutlet: string, listTrans: string[]) => {
  return new Promise<void>((resolve, reject) => {
    conn.query("DELETE FROM tbltranskaskeluar WHERE kodeoutlet = ? AND idtrans IN (?)", [kodeoutlet, listTrans], (err) => {
      if (err) return reject(err)
      conn.query("DELETE FROM tbltranskaskeluard WHERE kodeoutlet = ? AND idtrans IN (?)", [kodeoutlet, listTrans], (err) => {
        if (err) return reject(err)
        return resolve()
      })
    })
  })
}

const processKasKeluar = (listKasKeluar: TransaksiKas[], kodeoutlet: string) => {
  return new Promise<void>((resolve, reject) => {
    if (listKasKeluar.length > 0) {
      db.getConnection((err, conn) => {
        if (err) return reject(err)
        conn.beginTransaction((err) => {
          if (err) return reject(err)
          hapusPreviousKasKeluar(conn, kodeoutlet, listKasKeluar.map(el => el._id)).then(() => {
            const hasilProcess = listKasKeluar.map(el => {
              return new Promise<void>((resolve, reject) => {
                const jamin = el.jamin != null ? moment(el.jamin).format("YYYY-MM-DD HH:mm:ss") : undefined;
                const tanggal = el.tanggal != null ? moment(el.tanggal).format("YYYY-MM-DD HH:mm:ss") : undefined
                conn.query("INSERT INTO tbltranskaskeluar (kodeoutlet,idtrans,noinvoice,sessionId,userin,jamin,tanggal) VALUE (?,?,?,?,?,?,?)", [kodeoutlet, el._id, el.noinvoice, el.sessionId, el.userin, jamin, tanggal], (err) => {
                  if (err) return reject(err)
                  const promises = el.detail.map(dt => {
                    return new Promise<void>((resolve, reject) => {
                      conn.query("INSERT INTO tbltranskaskeluard (kodeoutlet, idtrans, noinvoice, kodebiaya, namabiaya, amount, keterangan) VALUE (?,?,?,?,?,?,?)", [kodeoutlet, el._id, el.noinvoice, dt.kodebiaya, dt.namabiaya, dt.amount, dt.keterangan], (err) => {
                        if (err) return reject()
                        else return resolve()
                      })
                    })
                  })
                  Promise.all<void>(promises).then(() => resolve()).catch(reject)
                })
              })
            })
            return Promise.all<void>(hasilProcess)
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

const hapusPreviousStock = (conn: Connection, kodeoutlet: string, idtrans: string[]) => {
  return new Promise<void>((resolve, reject) => {
    const banyakTrans = idtrans.length
    const jumlahLoop = Math.ceil(banyakTrans / 1000)
    const listPromise = []
    for (let i = 0; i < jumlahLoop; i++) {
      const split = idtrans.slice((i * 1000), (i * 1000) + 999)
      listPromise.push(new Promise<void>((resolve, reject) => {
        conn.query("UPDATE tblstockdetail SET `status` = 8, userupt = 'system', jamupt = NOW() WHERE kodeoutlet = ? AND referencecode IN (?) AND `status` = 1", [kodeoutlet, split], (err) => {
          if (err) return reject(err)
          return resolve()
        })
      }))
    }
    Promise.all(listPromise).then(() => resolve()).catch(err => reject(err))
  })
}

const processStock = (kodeoutlet: string, listTransaksi: Transaksi[]) => {
  return new Promise<void>((resolve, reject) => {
    console.log(`PROCESSING ${listTransaksi.length} STOCK TRANSACTION`)
    if (listTransaksi.length > 0) {
      db.getConnection((err, conn) => {
        if (err) {
          conn.release()
          return reject(err)
        }
        conn.beginTransaction((err) => {
          if (err) {
            conn.release();
            return reject(err)
          }
          conn.config.timeout = 0
          hapusPreviousStock(conn, kodeoutlet, listTransaksi.map(el => el._id)).then(() => {
            const banyakTrans = listTransaksi.length
            const jumlahLoop = Math.ceil(banyakTrans / 1000)
            const listPromise = []
            for (let i = 0; i < jumlahLoop; i++) {
              const split = listTransaksi.slice((i * 1000), (i * 1000) + 999)
              listPromise.push(new Promise<void>((resolve, reject) => {
                const hasilProcess = split.map(el => {
                  return new Promise<void>((resolve, reject) => {
                    if ([20, 9].indexOf(el.statusid) > -1 && el.useDelivery === false) {
                      const item = el.detail.map(dt => {
                        return new Promise<void>((resolve, reject) => {
                          conn.query("INSERT INTO tblstockdetail (kodeoutlet,kodebarang,qty,satuan,tanggal,`status`,userin,jamin,referencecode) VALUE(?,?,?,?,?,1,'SYSTEM',NOW(),?)", [kodeoutlet, dt.kodebarang, dt.qty * -1, "PCS", moment(el.tanggal).format("YYYY-MM-DD HH:mm:ss"), el._id], (err) => {
                            if (err) return reject(err)
                            else return resolve();
                          })
                        })
                      })
                      Promise.all(item).then(() => resolve()).catch(reject)
                    } else return resolve();
                  })
                })
                Promise.all<void>(hasilProcess).then(() => resolve()).catch(reject)
              }))
            }
            return Promise.all<void>(listPromise)
          }).then(() => {
            console.log("SELESAI PROSES STOCK")
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

const importTransaksi = (kodeoutlet: string, listTransaksi: Transaksi[]) => {
  return new Promise<void>((resolve, reject) => {
    if (listTransaksi.length === 0) return resolve();
    const listTransaksiTerposting = listTransaksi.filter(el => el.statusid === 20 || el.statusid === 9);
    if (listTransaksiTerposting.length === 0) return resolve();
    db.getConnection((err, conn) => {
      if (err) return reject(err);
      conn.query('SELECT mysqlUser, mysqlHost, mysqlDb, mysqlPwd, salt FROM tbloutlet WHERE kodeoutlet = ? LIMIT 1', [kodeoutlet], (err, results) => {
        conn.release();
        if (err) return reject(err);
        if (results && results.length > 0) {
          const [{ mysqlUser, mysqlHost, mysqlDb, mysqlPwd, salt }] = results;
          if (Boolean(mysqlUser) && Boolean(mysqlHost) && Boolean(mysqlDb) && Boolean(mysqlPwd)) {
            const myconn = dbmid(mysqlHost, mysqlUser, mysqlPwd, mysqlDb);
            const promKodeOutlet = new Promise<string>((resolveKodeOutlet, rejectKodeOutlet) => {
              const token = sha256(`${salt}${kodeoutlet}${salt}`)
              myconn.query('SELECT kodeoutlet FROM tbloutlet WHERE idoutlet = ? LIMIT 1', [token], (err, results) => {
                if (err) return rejectKodeOutlet(err);
                if (results && results.length > 0) {
                  return resolveKodeOutlet(results[0].kodeoutlet);
                } else return rejectKodeOutlet(new Error('Outlet Tidak Ditemukan'));
              });
            });
            promKodeOutlet
              .then(kodeOutletERP => {
                myconn.beginTransaction((err) => {
                  if (err) {
                    myconn.end();
                    return reject(err);
                  } else {
                    const promDeletePayment = new Promise<void>((resolve2, reject2) => {
                      myconn.query('DELETE p FROM tblorderanpayment p WHERE p.kodeorderan IN (?)', [listTransaksiTerposting.map(el => `${el.noinvoice}-${kodeOutletERP}`)], (err, results) => {
                        if (err) {
                          return reject2(err);
                        }
                        if (results.affectedRows >= 0) {
                          return resolve2();
                        } else return reject2(new Error('Error on deleting previous record'));
                      });
                    });
                    promDeletePayment
                      .then(() => {
                        return new Promise<void>((resolve2, reject2) => {
                          myconn.query('DELETE p FROM tblorderand p WHERE p.kodeorderan IN (?)', [listTransaksiTerposting.map(el => `${el.noinvoice}-${kodeOutletERP}`)], (err, results) => {
                            if (err) {
                              return reject2(err);
                            }
                            if (results.affectedRows >= 0) {
                              return resolve2();
                            } else return reject2(new Error('Error on deleting previous record'));
                          });
                        });
                      })
                      .then(() => {
                        return new Promise<void>((resolve2, reject2) => {
                          myconn.query('DELETE p FROM tblorderan p WHERE p.kodeorderan IN (?)', [listTransaksiTerposting.map(el => `${el.noinvoice}-${kodeOutletERP}`)], (err, results) => {
                            if (err) {
                              return reject2(err);
                            }
                            if (results.affectedRows >= 0) {
                              return resolve2();
                            } else return reject2(new Error('Error on deleting previous record'));
                          });
                        });
                      })
                      .then(() => {
                        const listPromise = listTransaksiTerposting
                          .map(el => {
                            return new Promise<void>((resolve2, reject2) => {
                              const promH = new Promise<void>((resolve3, reject3) => {
                                const diskon = el.payment.map(pay => {
                                  if (pay.noinvoice.indexOf('split-to-') < 0) {
                                    return pay.discountAmount
                                  } else return 0;
                                }).reduce((a, b) => a + b, 0);
                                const service = el.payment.map(pay => {
                                  if (pay.noinvoice.indexOf('split-to-') < 0) {
                                    return pay.serviceAmount
                                  } else return 0
                                }).reduce((a, b) => a + b, 0);
                                const tax = el.payment.map(pay => {
                                  if (pay.noinvoice.indexOf('split-to-') < 0) {
                                    return pay.taxAmount
                                  } else return 0
                                }).reduce((a, b) => a + b, 0);
                                const ongkir = el.payment.map(pay => {
                                  if (pay.noinvoice.indexOf('split-to-') < 0) {
                                    return pay.ongkir || 0
                                  } else return 0;
                                }).reduce((a, b) => a + b, 0);
                                const pembulatan = el.payment.map(pay => {
                                  if (pay.noinvoice.indexOf('split-to-') < 0) {
                                    return pay.pembulatan || 0
                                  } else return 0;
                                }).reduce((a, b) => a + b, 0);
                                const jumlah = el.payment.map(pay => {
                                  if (pay.noinvoice.indexOf('split-to-') < 0) {
                                    return pay.item.map(item => {
                                      return item.qty * (item.harga * (100 - item.discountPercent) / 100)
                                    }).reduce((a, b) => a + b, 0);
                                  } else return 0;
                                }).reduce((a, b) => a + b, 0);
                                const grandtot = jumlah - diskon + tax + service + ongkir + pembulatan;
                                let diskonpers = 0;
                                let servicepers = 0;
                                let taxpers = 0;
                                if (jumlah !== 0) {
                                  diskonpers = diskon / jumlah * 100
                                }
                                if (jumlah - diskon !== 0) {
                                  servicepers = service / (jumlah - diskon) * 100;
                                  taxpers = tax / (jumlah - diskon + service) * 100;
                                } else {
                                  if (el.payment.length > 0) {
                                    servicepers = el.payment[0].servicePercent;
                                    taxpers = el.payment[0].taxPercent;
                                  }
                                }
                                //INSERT HEADER
                                myconn.query('INSERT INTO tblorderan (kodeorderan,tanggal,statusid,diskon,total,grandtot,userin,userupt,jam,jamupt,diskpers,periode,serv,persenserv,tax,persentax,ket,nomejamanual,kodegudang,outlet) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', [`${el.noinvoice}-${kodeOutletERP}`, moment(el.tanggal).format('YYYY-MM-DD HH:mm:ss'), 20, diskon, jumlah, grandtot, el.userin, el.userupt, moment(el.jamin).format('YYYY-MM-DD HH:mm:ss'), moment(el.jamupt).format('YYYY-MM-DD HH:mm:ss'), diskonpers, moment(el.tanggal).format('YYYYMM'), service, servicepers, tax, taxpers, el.namacustomer, (el.nomeja || ''), 'GEN0001', kodeOutletERP], (err, results) => {
                                  if (err) return reject3(err);
                                  if (results.affectedRows > 0) {
                                    return resolve3();
                                  } else return reject3(new Error('Failed to insert data'));
                                });
                              });
                              promH
                                .then(() => {
                                  const listDetail = el.payment.map(det => {
                                    return new Promise<void>((resolve3, reject3) => {
                                      if (det.noinvoice.indexOf('split-to-') < 0) {
                                        const listMoreDetail = det.item.map(item => {
                                          return new Promise<void>((resolve4, reject4) => {
                                            myconn.query('INSERT INTO tblorderand (kodeorderan,kodebarang,qty,harga,jumlah,satuan,qtysat,hargajual,diskon) VALUES (?,?,?,?,?,?,?,?,?);', [`${el.noinvoice}-${kodeOutletERP}`, item.kodebarang, item.qty, item.harga, item.jumlah, 'pcs', item.qty, item.harga, item.discountAmount], (err, results) => {
                                              if (err) return reject4(err);
                                              if (results.affectedRows > 0) return resolve4();
                                              else return reject4(new Error('Failed to insert data'));
                                            });
                                          });
                                        });
                                        Promise
                                          .all(listMoreDetail)
                                          .then(() => resolve3())
                                          .catch(reject3);
                                      } else return resolve3();
                                    });
                                  });
                                  return Promise.all(listDetail);
                                })
                                .then(() => {
                                  const listPayment = el.payment.map(det => {
                                    return new Promise<void>((resolve3, reject3) => {
                                      if (det.noinvoice.indexOf('split-to-') < 0) {
                                        const listMoreDetail = det.payment.map(pay => {
                                          return new Promise<void>((resolve4, reject4) => {
                                            myconn.query('INSERT INTO tblorderanpayment (kodeorderan,kodepayment,jumlah,userin,userupt,jamin,jamupt,namapayment,nobukti) VALUES (?,?,?,?,?,?,?,?,?)', [`${el.noinvoice}-${kodeOutletERP}`, pay.kodepayment, pay.amount, det.userin, det.userin, moment(det.jamin).format('YYYY-MM-DD HH:mm:ss'), moment(det.jamin).format('YYYY-MM-DD HH:mm:ss'), pay.namapayment, pay.transRef !== null ? pay.transRef : ''], (err, results) => {
                                              if (err) return reject4(err);
                                              if (results.affectedRows > 0) return resolve4();
                                              else return reject4(new Error('Failed to insert data'));
                                            });
                                          });
                                        });
                                        Promise
                                          .all(listMoreDetail)
                                          .then(() => resolve3())
                                          .catch(reject3);
                                      } else return resolve3();
                                    });
                                  });
                                  return Promise.all(listPayment)
                                })
                                .then(() => resolve2())
                                .catch(reject2);
                            });
                          });
                        return Promise.all(listPromise);
                      })
                      .then(() => {
                        myconn.commit((err) => {
                          if (err) {
                            myconn.rollback(() => {
                              myconn.end();
                              return reject(err);
                            });
                          } else {
                            myconn.end();
                            return resolve();
                          }
                        });
                      })
                      .catch(err => {
                        myconn.rollback(() => {
                          myconn.end();
                          return reject(err);
                        });
                      });
                  }
                });
              }).catch(err => {
                myconn.end();
                return reject(err);
              });
          } else {
            console.log('Database not configured. Skipping');
            return resolve();
          }
        } else {
          return reject(new Error('Outlet not found'));
        }
      });
    });
  });
}

const prosesERPSync = (mysqlConfig: MysqlInfo) => {
  return new Promise<void>((resolve, reject) => {
    const myconn = dbmid(mysqlConfig.host, mysqlConfig.user, mysqlConfig.password, mysqlConfig.dbname);
    myconn.beginTransaction(() => {
      const listCheck = [];
      listCheck.push(
        new Promise((resolveCheckOrderan, rejectCheckOrderan) => {
          myconn.query('SHOW FIELDS FROM tblorderan WHERE `Field` = \'kodeorderan\';', (err, results) => {
            console.log("results", results, mysqlConfig.host, mysqlConfig.user, mysqlConfig.password, mysqlConfig.dbname)
            if (err) return rejectCheckOrderan(err);
            if (results && results.length > 0) {
              const [result] = results;
              return resolveCheckOrderan(result);
            } else {
              return rejectCheckOrderan(new Error('Tidak menemukan field untuk tblorderan - kodeorderan'));
            }
          });
        })
      );
      listCheck.push(
        new Promise((resolveCheckOrderanD, rejectCheckOrderanD) => {
          myconn.query('SHOW FIELDS FROM tblorderand WHERE `Field` = \'kodeorderan\';', (err, results) => {
            if (err) return rejectCheckOrderanD(err);
            if (results && results.length > 0) {
              const [result] = results;
              return resolveCheckOrderanD(result);
            } else {
              return rejectCheckOrderanD(new Error('Tidak menemukan field untuk tblorderand - kodeorderan'));
            }
          });
        })
      );
      listCheck.push(
        new Promise((resolveCheckStockDetail, rejectCheckStockDetail) => {
          myconn.query('SHOW FIELDS FROM tblstockdetail WHERE `Field` = \'kodetransaksi\';', (err, results) => {
            if (err) return rejectCheckStockDetail(err);
            if (results && results.length > 0) {
              const [result] = results;
              return resolveCheckStockDetail(result);
            } else {
              return rejectCheckStockDetail(new Error('Tidak menemukan field untuk tblstockdetail2 - kodetransaksi'));
            }
          });
        })
      );
      listCheck.push(
        new Promise((resolveCheckStockDetail2, rejectCheckStockDetail2) => {
          myconn.query('SHOW FIELDS FROM tblstockdetail2 WHERE `Field` = \'kodetransaksi\';', (err, results) => {
            if (err) return rejectCheckStockDetail2(err);
            if (results && results.length > 0) {
              const [result] = results;
              return resolveCheckStockDetail2(result);
            } else {
              return rejectCheckStockDetail2(new Error('Tidak menemukan field untuk tblstockdetail2 - kodetransaksi'));
            }
          });
        })
      );
      listCheck.push(
        new Promise((resolveCheckOrderanPayment, rejectCheckOrderanPayment) => {
          myconn.query('SHOW FIELDS FROM tblorderanpayment WHERE `Field` = \'kodeorderan\';', (err, results) => {
            if (err) return rejectCheckOrderanPayment(err);
            if (results && results.length > 0) {
              const [result] = results;
              return resolveCheckOrderanPayment(result);
            } else {
              return rejectCheckOrderanPayment(new Error('Tidak menemukan field untuk tblorderanpayment - kodeorderan'));
            }
          });
        })
      );
      Promise
        .all(listCheck)
        .then(([fieldOrderan, fieldOrderanD, fieldStockDetail, fieldStockDetail2, fieldOrderanPayment]) => {
          return new Promise<void>((resolveSinkronField, rejectSinkronField) => {
            const checkFieldOrderan = fieldOrderan.Type.toString().toLowerCase() === 'varchar(200)';
            const checkFieldOrderanD = fieldOrderanD.Type.toString().toLowerCase() === 'varchar(200)';
            const checkFieldStockDetail = fieldStockDetail.Type.toString().toLowerCase() === 'varchar(200)';
            const checkFieldStockDetail2 = fieldStockDetail2.Type.toString().toLowerCase() === 'varchar(200)';
            const checkFieldOrderanPayment = fieldOrderanPayment.Type.toString().toLowerCase() === 'varchar(200)';
            const listSinkron = [];
            if (!checkFieldOrderan) {
              listSinkron.push(
                new Promise<void>((resolveSinkronOrderan, rejectSinkronOrderan) => {
                  myconn.query('ALTER TABLE tblorderan MODIFY kodeorderan varchar(200);', err => {
                    if (err) return rejectSinkronOrderan(err);
                    return resolveSinkronOrderan();
                  });
                })
              );
            }
            if (!checkFieldOrderanD) {
              listSinkron.push(
                new Promise<void>((resolveSinkronOrderanD, rejectSinkronOrderanD) => {
                  myconn.query('ALTER TABLE tblorderand MODIFY kodeorderan varchar(200);', err => {
                    if (err) return rejectSinkronOrderanD(err);
                    return resolveSinkronOrderanD();
                  });
                })
              )
            }
            if (!checkFieldStockDetail) {
              listSinkron.push(
                new Promise<void>((resolveStockDetail, rejectStockDetail) => {
                  myconn.query('ALTER TABLE tblstockdetail MODIFY kodetransaksi varchar(200);', err => {
                    if (err) return rejectStockDetail(err);
                    return resolveStockDetail();
                  });
                })
              );
            }
            if (!checkFieldStockDetail2) {
              listSinkron.push(
                new Promise<void>((resolveStockDetail2, rejectStockDetail2) => {
                  myconn.query('ALTER TABLE tblstockdetail2 MODIFY kodetransaksi varchar(200);', err => {
                    if (err) return rejectStockDetail2(err);
                    return resolveStockDetail2();
                  });
                })
              );
            }
            if (!checkFieldOrderanPayment) {
              listSinkron.push(
                new Promise<void>((resolveSinkronOrderanPayment, rejectSinkronOrderanPayment) => {
                  myconn.query('ALTER TABLE tblorderanpayment MODIFY kodeorderan varchar(200);', err => {
                    if (err) return rejectSinkronOrderanPayment(err);
                    return resolveSinkronOrderanPayment();
                  });
                })
              );
            }
            if (listSinkron.length > 0) {
              Promise
                .all(listSinkron)
                .then(() => resolveSinkronField())
                .catch(err => rejectSinkronField(err));
            } else {
              return resolveSinkronField();
            }
          });
        })
        .then(() => {
          myconn.commit(() => {
            myconn.end();
            return resolve();
          });
        })
        .catch(err => {
          myconn.rollback(() => {
            myconn.end();
            return reject(err);
          });
        });
    });
  });
};

const processERPStock = (mysqlConfig: MysqlInfo, listTransaksi: Transaksi[], kodeoutlet: string) => {
  return new Promise<void>((resolve, reject) => {
    if (listTransaksi && listTransaksi.length > 0) {
      const myconn = dbmid(mysqlConfig.host, mysqlConfig.user, mysqlConfig.password, mysqlConfig.dbname);
      myconn.beginTransaction(() => {
        const listPromise = listTransaksi.map(trx => {
          return new Promise<void>((resolveT, rejectT) => {
            if (trx.payment && trx.payment.length > 0) {
              const listPayment = trx.payment.map(pay => {
                return new Promise<void>((resolveP, rejectP) => {
                  myconn.query('DELETE FROM tblstockdetail2 WHERE kodetransaksi = ?', [`${trx.noinvoice}-${kodeoutlet}`], (err, results) => {
                    if (err) return rejectP(err);
                    const listItem = (pay.item || []).map(item => {
                      return new Promise<void>((resolveI, rejectI) => {
                        myconn.query('SELECT kodebarang, qty, satuan FROM tblformulad WHERE kodeformula = ? AND statusid NOT IN (1,8)', [item.kodebarang], (err, results) => {
                          if (err) return rejectI(err);
                          if (results && results.length > 0) {
                            const listFormula = results.map(frm => {
                              const { kodebarang, qty, satuan } = frm;
                              return new Promise<void>((resolveF, rejectF) => {
                                myconn.query('INSERT INTO tblstockdetail2 (kodetransaksi, tanggal, kodebarang, qty, satuan, jenistrans, kodegudang) VALUES (?,?,?,?,?,\'1090\',?)', [`${trx.noinvoice}-${kodeoutlet}`, moment(pay.jamin).format('YYYY-MM-DD HH:mm:ss'), kodebarang, item.qty * qty * -1, satuan, kodeoutlet], (err, results) => {
                                  if (err) return rejectF(err);
                                  if (results && results.affectedRows > 0) return resolveF();
                                  else return rejectF(new Error('Failed to save stock'));
                                });
                              });
                            });
                            Promise
                              .all(listFormula)
                              .then(() => {
                                return new Promise<void>((resolveFG, rejectFG) => {
                                  myconn.query('INSERT INTO tblstockdetail2 (kodetransaksi, tanggal, kodebarang, qty, satuan, jenistrans, kodegudang) VALUES (?,?,?,?,1,\'1090\',?)', [`${trx.noinvoice}-${kodeoutlet}`, moment(pay.jamin).format('YYYY-MM-DD HH:mm:ss'), item.kodebarang, item.qty * -1, kodeoutlet], (err, results) => {
                                    if (err) return rejectFG(err);
                                    if (results && results.affectedRows > 0) return resolveFG();
                                    else return rejectFG(new Error('Failed to save stock'));
                                  });
                                });
                              })
                              .then(() => resolveI())
                              .catch(err => rejectI(err));
                          } else {
                            myconn.query('INSERT INTO tblstockdetail2 (kodetransaksi, tanggal, kodebarang, qty, satuan, jenistrans, kodegudang) VALUES (?,?,?,?,1,\'1090\',?)', [`${trx.noinvoice}-${kodeoutlet}`, moment(pay.jamin).format('YYYY-MM-DD HH:mm:ss'), item.kodebarang, item.qty * -1, kodeoutlet], (err, results) => {
                              if (err) return rejectI(err);
                              if (results && results.affectedRows > 0) return resolveI();
                              else return reject(new Error('Failed to save stock'));
                            });
                          }
                        });
                      });
                    });
                    Promise
                      .all(listItem)
                      .then(() => resolveP())
                      .catch(err => rejectP(err));
                  });
                });
              });
              Promise
                .all(listPayment)
                .then(() => resolveT())
                .catch(err => rejectT(err));
            } else return resolveT();
          });
        });
        Promise
          .all(listPromise)
          .then(() => {
            myconn.commit(() => {
              myconn.end();
              return resolve();
            });
          })
          .catch(err => {
            myconn.rollback(() => {
              myconn.end();
              return reject(err);
            });
          });
      });
    } else return resolve();
  });
}

const processOutlet = (headofficecode: string, database: string, kodeoutlet: string, namaoutlet: string, userdb: string, pwddb: string, useIposStock: boolean, autoImportTransaksi: boolean, useERPStock: boolean, mysql: MysqlInfo) => {
  // eslint-disable-next-line no-async-promise-executor
  return new Promise<void>(async (resolve, reject) => {
    let last_seq = ''
    last_seq = await getLastSequence(headofficecode, database);
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
    const host = process.env.couchdbsource || 'db.ipos5.co.id'
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

          let listDataPayment: PaymentOutlet[], listKategori: Kategori[], listSubkategori: SubKategori[], listBarang: Barang[], listTopping: Topping[];
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
          const listTransaksi: Transaksi[] = resp.results.map(el => {
            return {
              ...el.doc
            }
          }).filter(el => el.tipe === "transaksi")
          if (listTransaksi.length > 0) {
            try {
              await processTransaksi(listTransaksi, kodeoutlet, listKategori, listSubkategori, listBarang, listDataPayment, listTopping, cdb)
            } catch (err) {
              console.log(err)
              cdb.close()
              return reject(err)
            }
          }
          console.log(`${listTransaksi.length} transaksi terproses`);
          if (useIposStock) {
            try {
              //only process transaction from iPOS 1.109 or above
              const filterTransaksi = listTransaksi.filter(el => {
                if (el.version) {
                  // console.log("Processed Version", el.version, cmp(el.version, '1.108'))
                  return cmp(el.version, '1.108') > -1
                } else return false
              })
              if (filterTransaksi.length > 0) {
                await processStock(kodeoutlet, filterTransaksi)
              }
              console.log(`${filterTransaksi.length} stock transaksi terproses`)
            } catch (err) {
              console.log(err)
              cdb.close()
              return reject(err)
            }
          }
          if (useERPStock || autoImportTransaksi) {
            await prosesERPSync(mysql);
          }
          if (useERPStock) {
            try {
              const filterTransaksi = listTransaksi.filter(el => {
                if (el.version) {
                  // console.log("Processed Version", el.version, cmp(el.version, '1.108'))
                  return cmp(el.version, '1.127') > -1
                } else return false
              });
              if (filterTransaksi.length > 0) {
                await processERPStock(mysql, filterTransaksi, kodeoutlet);
              }
              console.log(`${filterTransaksi.length} stock ERP terproses`);
            } catch (err) {
              console.log(err);
              cdb.close();
              return reject(err);
            }
          }
          if (autoImportTransaksi && listTransaksi.length > 0) {
            try {
              await importTransaksi(kodeoutlet, listTransaksi);
            } catch (err) {
              console.log(err);
              cdb.close();
              return reject(err);
            }
          }
          const listSession: Session[] = resp.results.map(el => {
            return {
              ...el.doc
            }
          }).filter(el => el.tipe === "session")
          if (listSession.length > 0) {
            try {
              await processSession(listSession, kodeoutlet);
            } catch (err) {
              console.log(err)
              cdb.close()
              return (reject(err))
            }
          }
          console.log(`${listSession.length} sesi terproses`);
          const listKasMasuk: TransaksiKas[] = resp.results.map(el => {
            return {
              ...el.doc
            }
          }).filter(el => el.tipe === "transkasmasuk")
          if (listKasMasuk.length > 0) {
            try {
              await processKasMasuk(listKasMasuk, kodeoutlet);
            } catch (err) {
              console.log(err)
              cdb.close()
              return reject(err)
            }
          }
          console.log(`${listKasMasuk.length} kas masuk terproses`);
          const listKasKeluar: TransaksiKas[] = resp.results.map(el => {
            return {
              ...el.doc
            }
          }).filter(el => el.tipe === "transkaskeluar")
          if (listKasKeluar.length > 0) {
            try {
              await processKasKeluar(listKasKeluar, kodeoutlet);
            } catch (err) {
              console.log(err)
              cdb.close()
              return reject(err)
            }
          }
          console.log(`${listKasKeluar.length} kas keluar terproses`);
        } else console.log(`Tidak ada transaksi yang diproses`)
        await updateLastSequence(resp, headofficecode, database, last_seq || '0');
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

const processPaymentRetail = (conn: Connection, kodeoutlet: string, payment: any, idtrans: string, listBarang: any[], listKategori: any[]) => {
  return new Promise<void>((resolve, reject) => {
    if (payment) {
      const hasil = new Promise<void>((resolve, reject) => {
        conn.query("INSERT INTO tblorderanpayment (kodeoutlet,idtrans,noinvoice,urutpayment,subtotal,discountAmount,discountPercent,serviceAmount,servicePercent,taxAmount,taxPercent,total,pembulatan,kembalian,totalPayment,userin,jamin,sessionId) VALUE(?,?,?,1,?,?,?,?,?,?,?,?,0,?,?,?,?,?)", [kodeoutlet, idtrans, payment.noinvoice, payment.subtotal, payment.diskonAmt, payment.discount, payment.serviceAmt, payment.service, payment.taxAmt, payment.tax, payment.total, payment.kembalian, payment.totalBayar, payment.userin, moment(payment.jamin).format("YYYY-MM-DD HH:mm:ss"), payment.sessionId], err => {
          if (err) return reject(err)
          const paymentItem = payment.item.map(el2 => {
            return new Promise<void>((resolve, reject) => {
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
          const paymentDetail = new Promise<void>((resolve, reject) => {
            const isCard = payment.paymentMethod.toUpperCase() !== 'CASH'
            conn.query("INSERT INTO tblorderanpaymentdetail (kodeoutlet,idtrans,noinvoice,urutpayment,kodepayment,namapayment,amount,isCard,isCashlez) VALUE (?,?,?,1,?,?,?,?,0)", [kodeoutlet, idtrans, payment.noinvoice, payment.kodepayment, payment.namapayment, payment.total, isCard], err => {
              if (err) return reject(err)
              return resolve()
            })
          })
          Promise.all<void>([
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

const processItemRetail = (conn: Connection, kodeoutlet: string, detail: any[], idtrans: string, noinvoice: string, listKategori: any[], listBarang: any[]) => {
  return new Promise<void>((resolve, reject) => {
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

const processOrderanRetail = (conn: Connection, kodeoutlet: string, idtrans: string, noinvoice: string, tanggal: string, userin: string, userupt: string, jamin: string, jamupt: string, lastJamBayar: string, statusid: string, voidReason: string, userRetur: string) => {
  return new Promise<void>((resolve, reject) => {
    const _lastJamBayar = lastJamBayar == null ? null : moment(lastJamBayar).format("YYYY-MM-DD HH:mm:ss")
    conn.query("INSERT INTO tblorderan (kodeoutlet,idtrans,noinvoice,tanggal,userin,userupt,jamin,jamupt,lastJamBayar,statusid,voidReason,userRetur, jenisOutlet) VALUE (?,?,?,?,?,?,?,?,?,?,?,?,'SIMPLE')", [kodeoutlet, idtrans, noinvoice, moment(tanggal).format("YYYY-MM-DD HH:mm:ss"), userin, userupt, moment(jamin).format("YYYY-MM-DD HH:mm:ss"), moment(jamupt).format('YYYY-MM-DD HH:mm:ss'), _lastJamBayar, statusid, voidReason, userRetur], (err) => {
      if (err) return reject(err)
      return resolve()
    })
  })
}

const processTransaksiRetail = (listTransaksi: any[], kodeoutlet: string, listKategori: any[], listBarang: any[]) => {
  return new Promise<void>((resolve, reject) => {
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
              return new Promise<void>((resolve, reject) => {
                const idtrans = el._id
                processOrderanRetail(conn, kodeoutlet, idtrans, el.noinvoice, el.tanggal, el.userin, el.userupt, el.jamin, el.jamupt, el.lastJamBayar, el.statusid, el.voidReason, el.userRetur).then(() => {
                  return processItemRetail(conn, kodeoutlet, el.detail, idtrans, el.noinvoice, listKategori, listBarang)
                }).then(() => {
                  return processPaymentRetail(conn, kodeoutlet, { paymentMethod: el.paymentMethod, kodepayment: (el.paymentMethod.toUpperCase() === "CASH" || el.paymentMethod.toUpperCase() === "TUNAI") ? "TUNAI" : el.kodepayment != null ? el.kodepayment : "NON TUNAI" /**el.kodepayment**/, namapayment: el.namapayment != null ? el.namapayment : (el.paymentMethod.toUpperCase() === "CASH" || el.paymentMethod.toUpperCase() === "TUNAI"), "TUNAI": "CREDIT" /**el.namapayment */, tax: el.tax, discount: el.discount, service: el.service, subtotal: el.subtotal, diskonAmt: el.diskonAmt, serviceAmt: el.serviceAmt, taxAmt: el.taxAmt, total: el.total, totalBayar: el.totalBayar, kembalian: el.kembalian, item: el.detail, noinvoice: el.noinvoice, sessionId: el.sessionId, userin: el.userin, jamin: el.jamin }, idtrans, listBarang, listKategori)
                }).then(() => {
                  return resolve()
                }).catch(err => {
                  console.log("Error", err)
                  // const fs = require('fs')
                  // fs.writeFile("D:\\error.txt", JSON.stringify(err), err2 => {
                  //   if (err2) console.log(err2)
                  // })
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

const processRetail = (headofficecode: string, database: string, kodeoutlet: string, namaoutlet: string, userdb: string, pwddb: string) => {
  return new Promise<void>(async (resolve, reject) => {
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
          await updateLastSequence(resp, headofficecode, database, last_seq);
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

const processByNama = (dbname: string): Promise<void> => {
  return new Promise<void>((resolve, reject) => {
    console.log("Process by Nama", dbname)
    db.getConnection((err, conn) => {
      if (err) return (reject(err))
      conn.query("SELECT headofficecode,`database`,kodeoutlet,namaoutlet,userdb,pwddb,useIposStock,tipeOutlet, autoImportTransaksi, useERPStock, mysqlUser, mysqlDb, mysqlPwd, mysqlHost FROM tbloutlet WHERE `status` <> 8 AND `database` = ?", [dbname], (err, results) => {
        conn.release()
        if (err) return (reject(err))
        if (results && results.length > 0) {
          const [outlet] = results;
          if (outlet.headofficecode) {
            switch (outlet.tipeOutlet) {
              case "NORMAL":
                const mysql: MysqlInfo = {
                  user: outlet.mysqlUser,
                  password: outlet.mysqlPwd,
                  host: outlet.mysqlHost,
                  dbname: outlet.mysqlDb,
                };
                console.log(`Processing data resto for outlet ${outlet.namaoutlet}`)
                processOutlet(outlet.headofficecode, outlet.database, outlet.kodeoutlet, outlet.namaoutlet, outlet.userdb, outlet.pwddb, outlet.useIposStock, outlet.autoImportTransaksi, outlet.useERPStock, mysql).then(() => {
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
            console.log('No head office attached');
            return (resolve());
          }
        } else {
          console.log(`No outlet found`)
          return (resolve())
        }
      })
    })
  })
}

const processByIdtrans = (kodeoutlet: string, idtrans: string): Promise<void> => {
  return new Promise<void>((resolve, reject) => {
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
            return cdb.get(idtrans).then((data: Transaksi) => {
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

const processByTanggal = (kodeoutlet: string, tanggal: string): Promise<void> => {
  return new Promise<void>((resolve, reject) => {
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
                tanggal: {
                  $gte: moment(`${tanggal} 00:00:00`, 'YYYY-MM-DD HH-mm-ss').toISOString(),
                  $lte: moment(`${tanggal} 23:59:59`, 'YYYY-MM-DD HH:mm:ss').toISOString(),
                },
                statusid: {
                  $gt: null,
                },
              },
              limit: 99999
            }).then(({ docs }: { docs: Transaksi[] }) => {
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

const processKasByIdTrans = (kodeoutlet: string, idtrans: string): Promise<void> => {
  return new Promise<void>((resolve, reject) => {
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
          cdb.get(idtrans).then((data: TransaksiKas) => {
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

const processKasByTanggal = (kodeoutlet: string, tanggal: string): Promise<void> => {
  return new Promise<void>((resolve, reject) => {
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
          }).then(({ docs }: { docs: TransaksiKas[] }) => {
            const data: { tipe: string, data: TransaksiKas[] }[] = []
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

const processSessionByTanggal = (kodeoutlet: string, tanggal: string): Promise<void> => {
  return new Promise<void>((resolve, reject) => {
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
              tipe: 'session',
              tanggalBuka: {
                $gte: moment(`${tanggal} 00:00:00`, 'YYYY-MM-DD HH-mm-ss').toISOString(),
                $lte: moment(`${tanggal} 23:59:59`, 'YYYY-MM-DD HH:mm:ss').toISOString()
              },
            },
            limit: 99999
          }).then(({ docs }: { docs: Session[] }) => {
            console.log(`Processing ${docs.length} of session for ${outlet.namaoutlet}`);
            return processSession(docs, kodeoutlet);
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
  });
}

const processImportTransaksi = (kodeoutlet: string, idtrans: string) => {
  return new Promise<void>((resolve, reject) => {
    db.getConnection((err, conn) => {
      if (err) return reject(err)
      conn.query("SELECT * FROM tbloutlet WHERE status <> 8 AND kodeoutlet = ? AND tipeOutlet = 'NORMAL' AND autoImportTransaksi = 1", [kodeoutlet], (err, results) => {
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
          cdb
            .get(idtrans).then((data: Transaksi) => {
              return importTransaksi(kodeoutlet, [data]);
            })
            .then(() => {
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


export = {
  outlet: processByNama,
  idtrans: processByIdtrans,
  tanggal: processByTanggal,
  kas: processKasByIdTrans,
  kastanggal: processKasByTanggal,
  sessiontanggal: processSessionByTanggal,
  cekDetail,
  processImportTransaksi,
}
// exports.outlet = processByNama
// exports.idtrans = processByIdtrans
// exports.tanggal = processByTanggal
// exports.kas = processKasByIdTrans
// exports.kastanggal = processKasByTanggal