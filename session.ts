import PouchDB from 'pouchdb';
import PouchDBFind from 'pouchdb-find';
PouchDB.plugin(PouchDBFind);
type SessionDoc = {
  _id: string,
  _rev: string | undefined,
  sessionId: string,
  saldoAwal: number,
  saldoAkhir?: number,
  userBuka: string,
  userTutup?: string,
  tanggalBuka: Date,
  tanggalTutup?: Date,
  tipe: string,
  version: string,
  [key: string]: any,
}
const syncSession = (data: SessionDoc, database: string, userdb: string, pwddb: string): Promise<void> => {
  return new Promise<void>((resolve, reject) => {
    const remoteDB = new PouchDB(`${process.env.couchdbtarget}/${database}`, {
      auth: {
        username: userdb,
        password: pwddb,
      }
    });
    remoteDB
      .find({
        selector: {
          _id: {
            $gt: null,
          },
          tipe: 'session',
          sessionId: data.sessionId,
        }
      })
      .then(resp => {
        if (resp.docs.length > 0) {
          console.log('Document found. skipping');
          remoteDB.close();
          return resolve();
        } else {
          console.log('Document not found. inserting');
          remoteDB
            .put({
              ...data,
              _rev: undefined,
              status: undefined,
            })
            .then(() => remoteDB.close())
            .then(() => resolve())
            .catch(err => reject(err));
        }
      })
  });
};

export ={
  syncSession,
}