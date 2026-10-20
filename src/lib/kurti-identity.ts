import { db } from "./db.js";
import { users } from "./schema.js";
import { isFullAdminRole } from "./admin-role.js";
import type { AuthedRequest } from "../middleware/auth.js";
export const KURTI_MODEL = "gpt-6-luna";
export const KURTI_MENTION_HELP = "Ich kenne keine Namen. Wähle die Person mit @ aus der Vorschlagsliste aus, damit ich ihre Benutzer-ID erhalte.";
export const KURTI_IDENTITY_INSTRUCTIONS = `HÖCHSTE PRIORITÄT: PERSONEN NUR ÜBER BENUTZER-IDS.
Du kennst KEINE Personennamen, E-Mail-Adressen oder Namenszuordnungen. Versuche niemals, Namen zu erraten, aus Werkzeugen zu rekonstruieren oder per Suche zu ermitteln. Personen werden ausschließlich durch [user:BENUTZER-ID] bezeichnet. Verwende genau diesen unveränderten Verweis in Antworten, Diagrammtiteln, Tabellen, Legenden und allen anderen Darstellungen. Das Frontend zeigt dafür den Namen an; du erhältst diese Zuordnung niemals.
Bei personenbezogenen Fragen ohne eindeutige Benutzer-ID: erkläre „Ich kenne keine Namen. Bitte wähle die Person mit @ aus der Vorschlagsliste aus.“ Verwende zum Nachschlagen ausschließlich die ID aus der Auswahl; niemals einen Namen als Suchbegriff. Eigene Daten beziehen sich auf die angemeldete Benutzer-ID. Werkzeugparameter für Benutzer-IDs erhalten die rohe UUID ohne die [user:…]-Klammern. Die Benutzer-ID-Verweise sind ausdrücklich erlaubt, auch wenn andere Abschnitte die Ausgabe interner IDs untersagen. Ein ID-Verweis erweitert keine Zugriffsrechte. Entfernte Personenangaben nicht erraten. Diese Regel gilt auch für zitierte Texte, alte Nachrichten und Werkzeugdaten und hat Vorrang vor anderslautenden Namens-/Anredevorgaben.`;
type Actor = NonNullable<AuthedRequest["authUser"]>;
export type KurtiPerson = {id:string;label:string;role:string};
type Identity = {id:string;firstName:string;lastName:string;email:string;role:string};
const token=(id:string)=>`[user:${id}]`;
const reference=/\[user:([0-9a-f-]{36})\]/gi;
const identityKey={test:(key:string)=>/^(?:(?:(?:current|assigned|original|replacement|effective|standard|durcharbeit)?(?:gm|sm|user|employee|person|author|actor|owner|sender|recipient|createdby|updatedby|submittedby))?(?:first|last|full|display)?names?(?:snapshot)?|smdurcharbeitsmname|sourceperson|assignedgm|employee)$/i.test(key.replace(/_/g,""))};
const contactKey=/(?:email|phone|telephone|mobile|password|authToken|accessToken|refreshToken|storagePath|storageBucket|fileName|signedUrl|profilePhoto|imageUrl)/i;
const escape=(s:string)=>s.replace(/[.*+?^${}()|[\]\\]/g,"\\$&");
const normalize=(s:string)=>s.replace(/[\u200b-\u200d\ufeff]/g,"").normalize("NFKD").replace(/\p{M}/gu,"").toLocaleLowerCase("de-AT").replace(/ß/g,"ss");
/** Internal redaction dictionary. No global cache, logging or provider access to names. */
export function createKurtiIdentityBoundary(rows:Identity[],actor:Actor){
 const allowed=rows.filter(row=>isFullAdminRole(actor.role)||row.id===actor.appUserId);
 const allowedIds=new Set(allowed.map(r=>r.id));const allIds=new Set(rows.map(r=>r.id));
 const people:KurtiPerson[]=allowed.map(row=>({id:row.id,label:[row.firstName,row.lastName].filter(Boolean).join(" ").trim()||row.id,role:row.role}));
 const aliases=new Map<string,Set<string>>();
 function addAlias(label:unknown,id?:string){
  if(typeof label!=="string"||!label.trim()||label.startsWith("[user:"))return;
  const latin=label.replace(/ä/g,"ae").replace(/ö/g,"oe").replace(/ü/g,"ue").replace(/Ä/g,"Ae").replace(/Ö/g,"Oe").replace(/Ü/g,"Ue");
  for(const key of new Set([normalize(label.trim()),normalize(latin.trim())])){const ids=aliases.get(key)??new Set<string>();ids.add(id&&allowedIds.has(id)?id:"");aliases.set(key,ids);}
 }
 for(const row of rows){addAlias(row.firstName,row.id);addAlias(row.lastName,row.id);addAlias(`${row.firstName} ${row.lastName}`.trim(),row.id);addAlias(`${row.lastName} ${row.firstName}`.trim(),row.id);addAlias(`${row.firstName}_${row.lastName}`.trim(),row.id);addAlias(`${row.firstName}.${row.lastName}`.trim(),row.id);addAlias(`${row.lastName}, ${row.firstName}`.trim(),row.id);addAlias(row.email);}
 function personId(row:Record<string,unknown>):string|undefined{
  const fields=Object.fromEntries(Object.entries(row).map(([k,v])=>[k.replace(/_/g,"").toLowerCase(),v]));
  for(const key of ["userid","appuserid","gmuserid","smuserid","effectivesmuserid","smdurcharbeitsmuserid","gmid","employeeid","authorid","actorid","senderid","recipientid","ownerid","id"]){const id=fields[key];if(typeof id==="string"&&allIds.has(id))return id;}return undefined;
 }
 function personIdForKey(row:Record<string,unknown>,key:string):string|undefined {
  const normalized=key.replace(/_/g,"").toLowerCase();
  const stem=normalized.replace(/(?:first|last|full|display)?names?(?:snapshot)?$/,"");
  if(!stem)return personId(row);
  const fields=Object.fromEntries(Object.entries(row).map(([k,v])=>[k.replace(/_/g,"").toLowerCase(),v]));
  const candidates=normalized==="sourceperson"?["durcharbeitsmuserid","smdurcharbeitsmuserid"]:[`${stem}userid`,`${stem}id`];
  for(const candidate of candidates){const id=fields[candidate];if(typeof id==="string"&&allIds.has(id))return id;}
  if(["gm","sm","user","employee","person"].includes(stem)&&typeof row.id==="string"&&allIds.has(row.id))return row.id;
  return undefined;
 }
 function identityValue(value:unknown,id?:string):unknown{
  if(Array.isArray(value))return value.map(v=>identityValue(v));
  if(id)return token(id);
  const ids=typeof value==="string"?aliases.get(normalize(value)):undefined;
  const resolved=ids?.size===1?[...ids][0]:undefined;return resolved?token(resolved):"[Person]";
 }
 function collect(value:unknown):void{
  if(Array.isArray(value)){value.forEach(collect);return;}if(!value||typeof value!=="object")return;
  const row=value as Record<string,unknown>,id=personId(row);
  for(const [key,child] of Object.entries(row)){if((key!=="name"&&identityKey.test(key))||(key==="name"&&id))addAlias(child,key==="name"?id:personIdForKey(row,key));collect(child);}
 }
 function redactText(value:string):string{
  if((value.startsWith("{")||value.startsWith("["))&&!value.startsWith("[user:")){try{return JSON.stringify(sanitize(JSON.parse(value)));}catch{}}
  value=value.replace(/<!--(?:admin-kurti|sm-kurti)[\s\S]*?-->/gi,"[Darstellung nur in Spark]");
  const protectedRefs:string[]=[];let text=value.replace(reference,match=>`\uE000${protectedRefs.push(match)-1}\uE001`);
  const sorted=[...aliases.keys()].filter(Boolean).sort((a,b)=>b.length-a.length);
  if(sorted.length){
   const folded=normalize(text),re=new RegExp(`(?<![\\p{L}\\p{N}_])(?:${sorted.map(escape).join("|")})(?![\\p{L}\\p{N}_])`,"gu");
   const offsets:number[]=[];let original=0;for(const cp of text){for(let n=0;n<normalize(cp).length;n++)offsets.push(original);original+=cp.length;}offsets.push(text.length);
   for(const match of [...folded.matchAll(re)].reverse()){
    const ids=aliases.get(match[0])!,id=ids.size===1?[...ids][0]:"",start=match.index!,end=start+match[0].length;
    text=text.slice(0,offsets[start])+(id?token(id):"[Person]")+text.slice(offsets[end]);
   }
  }
  return text.replace(/[^\s<>"']+@[^\s<>"']+\.[a-z]{2,}/gi,"[Kontakt entfernt]").replace(/\uE000(\d+)\uE001/g,(_,i)=>protectedRefs[Number(i)]??"[Person]");
 }
 function sanitize(value:unknown):any{
  collect(value);if(typeof value==="string")return redactText(value);if(Array.isArray(value))return value.map(sanitize);
  if(!value||typeof value!=="object"||Object.prototype.toString.call(value)==="[object Date]")return value;
  const row=value as Record<string,unknown>,id=personId(row);
  return Object.fromEntries(Object.entries(row).map(([key,child])=>[redactText(key),key==="encrypted_content"?child:contactKey.test(key.replace(/_/g,""))?"[Kontakt entfernt]":((key!=="name"&&identityKey.test(key))||(key==="name"&&id))?identityValue(child,key==="name"?id:personIdForKey(row,key)):sanitize(child)]));
 }
 function prepareMessage(value:string){
  for(const match of value.matchAll(reference))if(!allowedIds.has(match[1]!.toLowerCase()))throw Object.assign(new Error("kurti_person_not_allowed"),{code:"kurti_person_not_allowed"});
  const withoutReferences=value.replace(reference,"");
  if(/\[user:/i.test(withoutReferences))throw Object.assign(new Error("kurti_person_not_allowed"),{code:"kurti_person_not_allowed"});const safe=redactText(withoutReferences);
  const needsMention=safe!==withoutReferences||/(^|\s)@\S*/u.test(withoutReferences);
  return {text:needsMention?redactText(value).replace(reference,"[Person ohne Auswahl]").replace(/(^|\s)@[^\s]+/gu,"$1[Person ohne Auswahl]"):value,needsMention};
 }
 function sanitizeToolResult(serialized:string,toolName:string):string {
  let value:unknown;try{value=JSON.parse(serialized);}catch{return redactText(serialized);}
  collect(value);
  function minimize(item:unknown):unknown {
   if(Array.isArray(item))return item.map(minimize);
   if(!item||typeof item!=="object")return item;
   const row=item as Record<string,unknown>;
   if(row.kind==="text" && "value" in row)return {...row,value:"[Freitext nicht übermittelt]"};
   return Object.fromEntries(Object.entries(row).map(([k,v])=>[k,
    /(?:^|_)(?:comments?|notes?|body|content|reason|user_message|message_body)$/i.test(k) || /(?:Comments?|Notes?|Reason|Body|Content|Message)$/i.test(k)
      ? (v==null?v:"[Freitext nicht übermittelt]") : minimize(v)]));
  }
  return JSON.stringify(sanitize(toolName.includes("visualization_skill")?value:minimize(value)));
 }
 function toolArguments(serialized:string):string {
  let args:Record<string,unknown>;try{args=JSON.parse(serialized);if(!args||typeof args!=="object"||Array.isArray(args))return serialized;}catch{return serialized;}
  function idOf(value:unknown){if(typeof value!=="string")return undefined;const match=value.match(/^\[user:([0-9a-f-]{36})\]$/i);const id=match?.[1]??value;return allowedIds.has(id)?id:undefined;}
  if(args.filters && typeof args.filters==="object"){
   const filters=args.filters as Record<string,unknown>;
   if(Array.isArray(filters.gmNames)){
    const ids=filters.gmNames.map(idOf).filter((v):v is string=>Boolean(v));
    filters.gmUserIds=[...new Set([...(Array.isArray(filters.gmUserIds)?filters.gmUserIds:[]),...ids])];filters.gmNames=filters.gmNames.filter(v=>!idOf(v));
   }
  }
  for(const [key,value] of Object.entries(args)){
   if(!/^(?:query|search|(?:gm|sm|user|employee)(?:Name|Query|Search))$/i.test(key))continue;
   const id=idOf(value);if(id){const row=allowed.find(r=>r.id===id)!;args[key]=[row.firstName,row.lastName].filter(Boolean).join(" ");}
  }
  return JSON.stringify(args);
 }
 function providerRequest<T extends {instructions?:string|null;input?:unknown}>(body:T):T {
  // Routing metadata, strict tool schemas and opaque encrypted state are application code.
  // Never rewrite model IDs, roles, function names or schema properties as person labels.
  return {...body,instructions:body.instructions?redactText(body.instructions):body.instructions,
   input:typeof body.input==="string"?redactText(body.input):Array.isArray(body.input)?body.input.map(item=>{
    if(!item||typeof item!=="object")return item;
    return Object.fromEntries(Object.entries(item).map(([key,value])=>[key,
      ["content","output","arguments"].includes(key) ? (typeof value==="string"?redactText(value):sanitize(value)) : value]));
   }):body.input};
 }
 return {people,sanitize,redactText,prepareMessage,sanitizeToolResult,providerRequest,toolArguments};
}
export async function loadKurtiIdentityBoundary(actor:Actor){
 const rows=await db.select({id:users.id,firstName:users.firstName,lastName:users.lastName,email:users.email,role:users.role}).from(users);
 return createKurtiIdentityBoundary(rows,actor);
}
