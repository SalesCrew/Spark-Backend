import {createTimeEntryPhotoFixture} from './time-entry-photo-fixture.js';
import {createHomeHandler} from './time-entry-photo-home.mjs';
const fixture=await createTimeEntryPhotoFixture();
await fixture.pg.query('delete from time_tracking_entries where gm_user_id=$1',[fixture.ids.gm]);
fixture.app.get('/__synthetic/user',(_req,res)=>res.json({user:{id:fixture.ids.gm,role:'gm',email:'gm@example.invalid',firstName:'Max',lastName:'Mustermann'},session:{accessToken:'synthetic-gm',refreshToken:'synthetic-gm',expiresAt:4102444800}}));
const readEntries=async()=>{const rows=await fixture.database.select().from((await import('../src/lib/schema.js')).timeTrackingEntries);return rows.filter(e=>e.gmUserId===fixture.ids.gm).map(e=>({...e,startAt:e.startAt?.toISOString()??null,endAt:e.endAt?.toISOString()??null}));};
fixture.app.use(createHomeHandler(readEntries,fixture.ids.gm));
const server=fixture.app.listen(4038,'127.0.0.1',()=>console.log('Isolated actual photo and time routes + disposable PGlite: 127.0.0.1:4038'));
process.on('SIGINT',()=>server.close(()=>{void fixture.close();}));
