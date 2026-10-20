import { randomUUID } from "node:crypto";
import type { Responses } from "openai/resources/responses/responses";
import { z } from "zod";
import { isIsoDate,isoDateToEpochDay } from "../sm-planning.shared.js";
const nullableText=z.string().trim().min(1).max(200).nullable(), nullableId=z.string().uuid().nullable();
const nullableDate=z.string().refine(isIsoDate).nullable();
const fields={
 dataset:z.enum(["markets","planning","targets","visits","times","answers"]),
 title:z.string().trim().min(1).max(100),
 filters:z.object({from:nullableDate,to:nullableDate,smUserId:nullableId,marketId:nullableId,scope:z.enum(["standard","SMDurcharbeit"]).nullable(),
  search:nullableText,region:nullableText,chain:nullableText,campaignId:nullableId,month:nullableDate}).strict()
};
function valid(a:{dataset:string;filters:z.infer<typeof fields.filters>},ctx:z.RefinementCtx){
 const f=a.filters;
 if(a.dataset!=="markets"&&a.dataset!=="targets"){
  if(!f.from||!f.to||f.to<f.from||isoDateToEpochDay(f.to)-isoDateToEpochDay(f.from)>366)ctx.addIssue({code:"custom",message:"Aufsteigender Datumsbereich bis 366 Tage erforderlich."});
 }else if(f.from||f.to)ctx.addIssue({code:"custom",message:"Dieser Auszug verwendet keinen Datumsbereich."});
 if(a.dataset==="targets"){
  if(!f.month||!f.month.endsWith("-01"))ctx.addIssue({code:"custom",message:"Kalendermonat erforderlich."});
 }else if(f.month||f.campaignId)ctx.addIssue({code:"custom",message:"Kampagne/Monat nur für Monatsziele."});
 if(a.dataset!=="markets"&&(f.region||f.chain))ctx.addIssue({code:"custom",message:"Region/Kette nur für den Marktauszug."});
 if(!["markets","visits"].includes(a.dataset)&&f.search)ctx.addIssue({code:"custom",message:"Suche nur für Märkte/Besuche."});
 if(a.dataset==="targets"&&(f.marketId||f.scope))ctx.addIssue({code:"custom",message:"Monatsziele verwenden nur Kampagne/Monat/SM."});
 if(a.dataset==="markets"&&f.marketId)ctx.addIssue({code:"custom",message:"Marktauszug verwendet die Marktsuche."});
}
export const smKurtiExportInputSchema=z.object(fields).strict().superRefine(valid);
export const smKurtiExportSchema=z.object({id:z.string().uuid(),...fields}).strict().superRefine(valid);
export type SmKurtiExport=z.infer<typeof smKurtiExportSchema>;
const uuid={type:["string","null"],format:"uuid"},text={type:["string","null"],minLength:1,maxLength:200},date={type:["string","null"],pattern:"^\\d{4}-\\d{2}-\\d{2}$"};
export const SM_KURTI_EXPORT_TOOL:Responses.FunctionTool={type:"function",name:"prepare_sm_excel_export",description:
 "Prepare a downloadable SM Excel card using server-calculated data. Dataset markets accepts search/chain/region/smUserId/scope. targets requires calendar month and optional campaignId/smUserId. Other datasets require from/to (max 366 days), optional smUserId/marketId/scope; visits also accepts search. All unused filters must be null. Download reads fresh authorized data; never passes through GM export endpoints.",
 strict:true,parameters:{type:"object",properties:{dataset:{type:"string",enum:["markets","planning","targets","visits","times","answers"]},title:{type:"string",minLength:1,maxLength:100},
 filters:{type:"object",properties:{from:date,to:date,smUserId:uuid,marketId:uuid,scope:{type:["string","null"],enum:["standard","SMDurcharbeit",null]},
 search:text,region:text,chain:text,campaignId:uuid,month:date},required:["from","to","smUserId","marketId","scope","search","region","chain","campaignId","month"],additionalProperties:false}},
 required:["dataset","title","filters"],additionalProperties:false}};
export function parseSmKurtiExport(raw:string):SmKurtiExport{return {...smKurtiExportInputSchema.parse(JSON.parse(raw)),id:randomUUID()};}
const pattern=/\n?<!--sm-kurti-exports-v1:([A-Za-z0-9_-]+)-->\s*$/;
export function appendSmKurtiExports(content:string,exports:SmKurtiExport[]){
 return exports.length?content+"\n<!--sm-kurti-exports-v1:"+Buffer.from(JSON.stringify(z.array(smKurtiExportSchema).max(3).parse(exports))).toString("base64url")+"-->":content;
}
export function parseSmKurtiStoredExports(content:string):{content:string;smExports:SmKurtiExport[]}{
 const match=content.match(pattern);if(!match)return {content,smExports:[]};
 try{return {content:content.replace(pattern,"").trimEnd(),smExports:z.array(smKurtiExportSchema).max(3).parse(JSON.parse(Buffer.from(match[1]!,"base64url").toString("utf8")))};}
 catch{return {content:content.replace(pattern,"").trimEnd(),smExports:[]};}
}

